package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.log.segment.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompactionTask<T, L extends Log<T>> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CompactionTask.class);

    private final List<L> segments;

    public CompactionTask(List<L> segments) {
        this.segments = segments;
    }

    @Override
    public void run() {
        try {
            if (level <= 0) {
                throw new IllegalArgumentException("Level must be greater than zero");
            }
            if (levels.depth() <= level) {
                throw new IllegalArgumentException("No level " + level);
            }

            int nextLevel = level + 1;
            List<L> levelSegments = new ArrayList<>(levels.segmentsForCompaction(level));

            long totalSize = levelSegments.stream().mapToLong(Log::size).sum();

            logger.info("Compacting {} from level {} using {}, new segment size: {}", Arrays.toString(levelSegments.stream().map(Log::name).toArray()), level, segmentCombiner.getClass().getSimpleName(), totalSize);

            newSegment = createSegmentInternal(nextLevel, levels.size(nextLevel), totalSize);

            if (!levels.requiresCompaction(level)) {
                logger.warn("Nothing to compact");
                if (levels.requiresCompaction(nextLevel)) {
                    scheduleCompaction(nextLevel);
                }

                if (levels.requiresCompaction(level) && level != 1) {
                    scheduleCompaction(level);
                }
                return;
            }
            List<Stream<T>> entriesStream = levelSegments.stream().map(Log::stream).collect(Collectors.toList());
            segmentCombiner.merge(entriesStream, newSegment::append);

            newSegment.roll();

            //TODO here it should delete after all readers of the segments are closed
            logger.info("Deleting old segments");

            levels.add(nextLevel, newSegment);
            levels.removeSegmentsFromCompaction(levelSegments);

            state.levels(levels.segmentNames());
            state.flush();

            logger.info("Compaction complete, current number of levels: {}", levels);

            if (levels.requiresCompaction(nextLevel)) {
                scheduleCompaction(nextLevel);
            }

            if (levels.requiresCompaction(level) && level != 1) {
                scheduleCompaction(level);
            }


        } catch (Exception e) {
            if (newSegment != null) {
                newSegment.delete();
            }
            logger.error("Failed to compact", e);
            throw e;
        }
    }
}
