package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.appender.level.Levels;
import io.joshworks.fstore.log.appender.merge.SegmentCombiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Compactor<T, L extends Log<T>> {

    private static final Logger logger = LoggerFactory.getLogger(Compactor.class);

    private final ExecutorService compactionScheduler = Executors.newSingleThreadExecutor();
    private final Map<Integer, ExecutorService> levelExecutors = new HashMap<>();
    private final SegmentCombiner<T> segmentCombiner;

    private final int maxSegmentsPerLevel;
    private Levels<T, L> levels;

    public Compactor(SegmentCombiner<T> segmentCombiner, int maxSegmentsPerLevel, Levels<T, L> levels) {
        this.segmentCombiner = segmentCombiner;
        this.maxSegmentsPerLevel = maxSegmentsPerLevel;
        this.levels = levels;
    }


    public void newSegmentRolled() {
        int level = 1;
        if (maxSegmentsPerLevel <= 0 || !levels.requiresCompaction(level)) {
            return;
        }
        logger.info("Scheduling compaction of level {}", level);
        compact(level);
    }


    public void compact(int level) {

    }
}
