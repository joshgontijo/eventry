package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.seda.EventContext;
import io.joshworks.fstore.core.seda.StageHandler;
import io.joshworks.fstore.log.appender.SegmentFactory;
import io.joshworks.fstore.log.appender.StorageProvider;
import io.joshworks.fstore.log.appender.merge.SegmentCombiner;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static io.joshworks.fstore.log.appender.compaction.Compactor.COMPACTION_CLEANUP_STAGE;

public class CompactionTask<T, L extends Log<T>> implements StageHandler<CompactionEvent<T, L>> {


    @Override
    public void onEvent(EventContext<CompactionEvent<T, L>> context) {

        CompactionEvent<T, L> data = context.data;

        String magic = data.magic;
        int level = data.level;
        int nextLevel = data.level + 1;
        File segmentFile = data.segmentFile;
        SegmentCombiner<T> combiner = data.combiner;
        List<L> segments = data.segments;
        DataReader dataReader = data.dataReader;
        Serializer<T> serializer = data.serializer;
        StorageProvider storageProvider = data.storageProvider;
        SegmentFactory<T, L> segmentFactory = data.segmentFactory;

        final Logger logger = LoggerFactory.getLogger("compaction-task-" + level);

        L target = null;
        try {

            long totalSize = segments.stream().mapToLong(Log::size).sum();

            String names = Arrays.toString(segments.stream().map(Log::name).toArray());
            logger.info("Compacting {} from level {} using {}, new segment logSize: {}", names, level, combiner.getClass().getSimpleName(), totalSize);

            for (int i = 0; i < segments.size(); i++) {
                L segment = segments.get(i);
                logger.info("Segment[{}] {} - logSize: {}, entries: {}", i, segment.name(), segment.size(), segment.entries());
            }

            long start = System.currentTimeMillis();

            Storage storage = storageProvider.create(segmentFile, totalSize);
            target = segmentFactory.createOrOpen(storage, serializer, dataReader, magic, Type.MERGE_OUT);

            combiner.merge(segments, target);

            target.roll(nextLevel);

            logger.info("Result Segment {} - logSize: {}, entries: {}", target.name(), target.size(), target.entries());

            logger.info("Compaction completed, took {}ms", (System.currentTimeMillis() - start));
            context.submit(COMPACTION_CLEANUP_STAGE, CompactionResult.success(segments, target, level));

        } catch (Exception e) {
            logger.error("Failed to compact", e);
            context.submit(COMPACTION_CLEANUP_STAGE, CompactionResult.failure(segments, target, level, e));
        }
    }

//    private static <T> Consumer<T> bufferedWriter(int logSize, Consumer<T> delegate) {
//        Consumer<T> buffer = item -> {
//
//        };
//    }

    //TODO this class required log segment to accept bytebuffer
    private final class BufferedConsumer implements Consumer<T> {

        private final int size;
        private final Serializer<T> serializer;
        private final Consumer<T> delegate;
        private List<ByteBuffer> data = new ArrayList<>();

        private BufferedConsumer(int size, Serializer<T> serializer, Consumer<T> delegate) {
            this.size = size;
            this.delegate = delegate;
            this.serializer = serializer;
        }

        @Override
        public void accept(T t) {

        }
    }

}
