package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Compactor<T, L extends Log<T>> {

    private static final Logger logger = LoggerFactory.getLogger(Compactor.class);

//    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Map<Integer, ExecutorService> levelExecutors = new HashMap<>();
    private File directory;
    private final SegmentCombiner<T> segmentCombiner;

    private final SegmentFactory<T, L> segmentFactory;
    private final StorageProvider storageProvider;
    private Serializer<T> serializer;
    private DataReader dataReader;
    private NamingStrategy namingStrategy;
    private final int maxSegmentsPerLevel;
    private Levels<T, L> levels;

    public Compactor(File directory,
                     SegmentCombiner<T> segmentCombiner,
                     SegmentFactory<T, L> segmentFactory,
                     StorageProvider storageProvider,
                     Serializer<T> serializer,
                     DataReader dataReader,
                     NamingStrategy namingStrategy,
                     int maxSegmentsPerLevel,
                     Levels<T, L> levels) {
        this.directory = directory;
        this.segmentCombiner = segmentCombiner;
        this.segmentFactory = segmentFactory;
        this.storageProvider = storageProvider;
        this.serializer = serializer;
        this.dataReader = dataReader;
        this.namingStrategy = namingStrategy;
        this.maxSegmentsPerLevel = maxSegmentsPerLevel;
        this.levels = levels;
    }


    public void newSegmentRolled() {
        int level = 1;
        if (!requiresCompaction(level)) {
            return;
        }
        logger.info("Scheduling possible compaction of level {}", level);
        compact(level);
    }

    //force compact ???
    private void compact(int level) {

        List<L> sources = getSegmentsForCompaction(level);
        if(sources.isEmpty()) {
            logger.warn("Nothing to compact");
            return;
        }

        //TODO fix file name (idx on segment)
        File targetFile = LogFileUtils.newSegmentFile(directory, namingStrategy, 0, level + 1);

        CompactionTask<T, L> task = new CompactionTask<>(sources, segmentCombiner, targetFile, storageProvider, segmentFactory, serializer, dataReader, level);

        levelExecutors.putIfAbsent(level, Executors.newSingleThreadExecutor());
        CompletableFuture.supplyAsync(task::merge, levelExecutors.get(level)).thenAccept(this::handleResult);

    }

    private synchronized List<L> getSegmentsForCompaction(int level) {
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

    private void handleResult(CompactionResult<T, L> result) {
        if (!result.successful()) {
            //TODO
            logger.error("Compaction error", result.exception);
            logger.info("Deleting failed merge result segment");
            result.target.delete();
            return;
        }

        synchronized (this) {
            for (L segment : result.sources) {
                logger.info("Deleting {}", segment.name());
                segment.delete();
            }
            levels.remove(result.level, result.sources);

            int nextLevel = result.level + 1;
            levels.add(nextLevel, result.target);

            if(requiresCompaction(result.level)) {
                compact(result.level);
            }
            if(requiresCompaction(nextLevel)) {
                compact(nextLevel);
            }
        }

    }

}
