package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.seda.EventContext;
import io.joshworks.fstore.core.seda.SedaContext;
import io.joshworks.fstore.core.seda.Stage;
import io.joshworks.fstore.log.LogFileUtils;
import io.joshworks.fstore.log.appender.SegmentFactory;
import io.joshworks.fstore.log.appender.StorageProvider;
import io.joshworks.fstore.log.appender.level.Levels;
import io.joshworks.fstore.log.appender.merge.SegmentCombiner;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import io.joshworks.fstore.log.segment.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.List;

public class Compactor<T, L extends Log<T>> {

    private static final Logger logger = LoggerFactory.getLogger(Compactor.class);

    static final String COMPACTION_CLEANUP_STAGE = "compaction-cleanup";
    static final String COMPACTION_MANAGER = "compaction-manager";

    private File directory;
    private final SegmentCombiner<T> segmentCombiner;

    private final SegmentFactory<T, L> segmentFactory;
    private final StorageProvider storageProvider;
    private Serializer<T> serializer;
    private DataReader dataReader;
    private NamingStrategy namingStrategy;
    private final int maxSegmentsPerLevel;
    private Levels<T, L> levels;
    private final SedaContext sedaContext;

    private final CompactionTask<T, L> compactionTask = new CompactionTask<>();

    public Compactor(File directory,
                     SegmentCombiner<T> segmentCombiner,
                     SegmentFactory<T, L> segmentFactory,
                     StorageProvider storageProvider,
                     Serializer<T> serializer,
                     DataReader dataReader,
                     NamingStrategy namingStrategy,
                     int maxSegmentsPerLevel,
                     Levels<T, L> levels,
                     SedaContext sedaContext) {
        this.directory = directory;
        this.segmentCombiner = segmentCombiner;
        this.segmentFactory = segmentFactory;
        this.storageProvider = storageProvider;
        this.serializer = serializer;
        this.dataReader = dataReader;
        this.namingStrategy = namingStrategy;
        this.maxSegmentsPerLevel = maxSegmentsPerLevel;
        this.levels = levels;
        this.sedaContext = sedaContext;

        sedaContext.addStage(COMPACTION_CLEANUP_STAGE, this::cleanup, new Stage.Builder().corePoolSize(1).maximumPoolSize(1));
        sedaContext.addStage(COMPACTION_MANAGER, this::onLevelUpdate, new Stage.Builder().corePoolSize(1).maximumPoolSize(1));
    }

    public void newSegmentRolled() {
        int level = 1;
        sedaContext.submit(COMPACTION_MANAGER, level);
    }

    private void onLevelUpdate(EventContext<Integer> event) {
        int level = event.data;

        List<L> segmentsForCompaction = getSegmentsForCompaction(level);
        if (segmentsForCompaction.isEmpty()) {
            logger.info("Nothing to compact on level {}, size: {}", level, levels.size(level));
            return;
        }
        logger.info("Compacting level {}", level);

        File targetFile = LogFileUtils.newSegmentFile(directory, namingStrategy, level + 1);

        String stageName = stageNameForLevel(level);
        if (!sedaContext.stages().contains(stageName)) {
            sedaContext.addStage(stageName, compactionTask, new Stage.Builder().corePoolSize(1).maximumPoolSize(1));
        }
        sedaContext.submit(stageName, new CompactionEvent<>(segmentsForCompaction, segmentCombiner, targetFile, segmentFactory, storageProvider, serializer, dataReader, level));
    }

    private String stageNameForLevel(int level) {
        return "compaction-level-" + level;
    }

    private List<L> getSegmentsForCompaction(int level) {
        if (level <= 0) {
            throw new IllegalArgumentException("Level must be greater than zero");
        }
        if (levels.depth() <= level) {
            throw new IllegalArgumentException("No level " + level);
        }
        if (!requiresCompaction(level)) {
            return Collections.emptyList();
        }
        return levels.segmentsForCompaction(level);
    }

    private boolean requiresCompaction(int level) {
        return maxSegmentsPerLevel > 0 && levels.requiresCompaction(level);
    }

    private void cleanup(EventContext<CompactionResult<T, L>> event) {
        CompactionResult<T, L> result = event.data;
        if (!result.successful()) {
            //TODO
            logger.error("Compaction error", result.exception);
            logger.info("Deleting failed merge result segment");
            result.target.delete();
            return;
        }

        int nextLevel = result.level + 1;
        levels.add(nextLevel, result.target);

        sedaContext.submit(COMPACTION_MANAGER, result.level);
        sedaContext.submit(COMPACTION_MANAGER, nextLevel);

        for (L segment : result.sources) {
            logger.info("Deleting {}", segment.name());
            segment.delete();
        }
        levels.remove(result.level, result.sources);
    }

}
