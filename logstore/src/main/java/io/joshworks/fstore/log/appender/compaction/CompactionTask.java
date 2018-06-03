package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.appender.SegmentFactory;
import io.joshworks.fstore.log.appender.StorageProvider;
import io.joshworks.fstore.log.appender.merge.SegmentCombiner;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompactionTask<T, L extends Log<T>> {

    private static final Logger logger = LoggerFactory.getLogger(CompactionTask.class);

    private final List<L> segments;
    private final SegmentCombiner<T> combiner;
    private File segmentFile;
    private SegmentFactory<T, L> segmentFactory;
    private StorageProvider storageProvider;
    private Serializer<T> serializer;
    private DataReader dataReader;
    private final int level;

    public CompactionTask(List<L> segments,
                          SegmentCombiner<T> combiner,
                          File segmentFile,
                          StorageProvider storageProvider,
                          SegmentFactory<T, L> segmentFactory,
                          Serializer<T> serializer,
                          DataReader dataReader,
                          int level) {
        this.segments = segments;
        this.combiner = combiner;
        this.segmentFile = segmentFile;
        this.segmentFactory = segmentFactory;
        this.storageProvider = storageProvider;
        this.serializer = serializer;
        this.dataReader = dataReader;
        this.level = level;
    }


    public CompactionResult<T, L> merge() {
        L target = null;
        try {
            int nextLevel = level + 1;
            long totalSize = segments.stream().mapToLong(Log::size).sum();

            String names = Arrays.toString(segments.stream().map(Log::name).toArray());
            logger.info("Compacting {} from level {} using {}, new segment size: {}", names, level, combiner.getClass().getSimpleName(), totalSize);

            long start = System.currentTimeMillis();

            Storage storage = storageProvider.create(segmentFile, totalSize);
            target = segmentFactory.createOrOpen(storage, serializer, dataReader, Type.MERGE_OUT);

            List<Stream<T>> entriesStream = segments.stream().map(Log::stream).collect(Collectors.toList());
            combiner.merge(entriesStream, target::append);

            target.roll(nextLevel);

            logger.info("Compaction completed, took {}ms", (System.currentTimeMillis() - start));
            return CompactionResult.success(segments, target, level);


        } catch (Exception e) {
            logger.error("Failed to compact", e);
            return CompactionResult.failure(segments, target, level, e);
        }
    }
}
