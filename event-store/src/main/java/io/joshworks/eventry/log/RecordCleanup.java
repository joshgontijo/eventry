package io.joshworks.eventry.log;

import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.eventry.stream.Streams;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.segment.Log;

import java.util.List;
import java.util.Optional;

public class RecordCleanup implements SegmentCombiner<EventRecord> {

    private final Streams streams;

    public RecordCleanup(Streams streams) {
        this.streams = streams;
    }

    @Override
    public void merge(List<? extends Log<EventRecord>> segments, Log<EventRecord> output) {
        if (segments.size() != 1) {
            throw new IllegalArgumentException("Scavenge can only be executed on a single segment at once");
        }

        Log<EventRecord> log = segments.get(0);

        try (LogIterator<EventRecord> iterator = log.iterator(Direction.FORWARD)) {

            while (iterator.hasNext()) {
                long oldPosition = iterator.position();

                EventRecord record = iterator.next();
                Optional<StreamMetadata> metadataOpt = streams.get(record.stream);
                if (!metadataOpt.isPresent()) {
                    //replace with a log warn
                    throw new RuntimeException("No metadataOpt available for stream: " + record.stream);
                }
                StreamMetadata metadata = metadataOpt.get();

                boolean streamDeleted = metadata.streamDeleted();
                if (streamDeleted) {
                    //skip record
                    continue;
                }

                boolean expired = metadata.maxAge > 0 && System.currentTimeMillis() - record.timestamp > metadata.maxAge;
                if (expired) {
                    //skip record
                    continue;
                }
                int currentStreamVersion = streams.version(metadata.hash);
                boolean obsolete = metadata.maxCount > 0 && currentStreamVersion - record.version > metadata.maxCount;
                if (obsolete) {
                    //skip record
                    continue;
                }

                long newPosition = output.append(record);
                System.out.println("Mapping from position " + oldPosition + " to " + newPosition);
                //TODO add position mapping to footer
                //TODO Create a new Segment Type to handle mapping
            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}
