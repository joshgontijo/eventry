package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.seda.EventContext;
import io.joshworks.fstore.core.seda.StageHandler;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.joshworks.fstore.log.appender.compaction.Compactor.COMPACTION_COMPLETED_STAGE;

public class CompactionTask<T, L extends Log<T>> implements StageHandler<CompactionEvent<T, L>> {


    @Override
    public void onEvent(EventContext<CompactionEvent<T, L>> context)  {

        CompactionEvent<T, L> data = context.data;

        final Logger logger = LoggerFactory.getLogger("compaction-task-" + data.level);

        L target = null;
        try {
            int nextLevel = data.level + 1;
            long totalSize = data.segments.stream().mapToLong(Log::size).sum();

            String names = Arrays.toString(data.segments.stream().map(Log::name).toArray());
            logger.info("Compacting {} from level {} using {}, new segment size: {}", names, data.level, data.combiner.getClass().getSimpleName(), totalSize);

            long start = System.currentTimeMillis();

            Storage storage = data.storageProvider.create(data.segmentFile, totalSize);
            target = data.segmentFactory.createOrOpen(storage, data.serializer, data.dataReader, Type.MERGE_OUT);

            List<Stream<T>> entriesStream = data.segments.stream().map(Log::stream).collect(Collectors.toList());
            data.combiner.merge(entriesStream, target::append);

            target.roll(nextLevel);

            logger.info("Compaction completed, took {}ms", (System.currentTimeMillis() - start));
            context.submit(COMPACTION_COMPLETED_STAGE, CompactionResult.success(data.segments, target, data.level));

        } catch (Exception e) {
            logger.error("Failed to compact", e);
            context.submit(COMPACTION_COMPLETED_STAGE, CompactionResult.failure(data.segments, target, data.level, e));
        }
    }
}
