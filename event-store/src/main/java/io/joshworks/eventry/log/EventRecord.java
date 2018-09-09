package io.joshworks.eventry.log;

import io.joshworks.eventry.data.Constant;
import io.joshworks.eventry.data.IndexFlushed;
import io.joshworks.eventry.data.LinkTo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.eventry.utils.StringUtils;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.charset.StandardCharsets;

public class EventRecord {

    public static final String STREAM_VERSION_SEPARATOR = "@";

    public final String stream;
    public final String type;
    public final int version;
    public final long timestamp;
    public final byte[] data;
    public final byte[] metadata;

    public EventRecord(String stream, String type, int version, long timestamp, byte[] data, byte[] metadata) {
        StringUtils.requireNonBlank(stream, "Stream must be provided");
        StringUtils.requireNonBlank(type, "Type must be provided");
        this.stream = stream;
        this.type = type;
        this.version = version;
        this.timestamp = timestamp;
        this.data = data;
        this.metadata = metadata;
    }

    public static EventRecord create(String stream, String type, String data) {
        return create(stream, type, StringUtils.toUtf8Bytes(data));
    }

    public static EventRecord create(String stream, String type, String data, String metadata) {
        return create(stream, type, StringUtils.toUtf8Bytes(data), StringUtils.toUtf8Bytes(metadata));
    }

    public static EventRecord create(String stream, String type, byte[] data) {
        return create(stream, type, data, new byte[0]);
    }

    public static EventRecord create(String stream, String type, byte[] data, byte[] metadata) {
        return new EventRecord(stream, type, -1, -1, data, metadata);
    }

    //TODO use in the response
    public String dataAsString() {
        return new String(data, StandardCharsets.UTF_8);
    }

    public String eventId() {
        return stream + STREAM_VERSION_SEPARATOR + version;
    }

    public boolean isSystemEvent() {
        return type.startsWith(Constant.SYSTEM_PREFIX);
    }

    public boolean isLinkToEvent() {
        return LinkTo.TYPE.equals(type);
    }


    @Override
    public String toString() {
        return "EventRecord{" + "stream='" + stream + '\'' +
                ", type='" + type + '\'' +
                ", version=" + version +
                ", timestamp=" + timestamp +
                '}';
    }

    public static class System {

//        private static final Serializer<StreamMetadata> streamSerializer = new StreamMetadataSerializer();

        private static final JsonSerializer<StreamMetadata> streamSerializer = JsonSerializer.of(StreamMetadata.class);
        private static final JsonSerializer<IndexFlushed> indexFlushed = JsonSerializer.of(IndexFlushed.class);

//
//
//        public static EventRecord createLinkTo(String stream, EventRecord event) {
//            return EventRecord.create(stream, LINKTO_TYPE, StringUtils.toUtf8Bytes(event.eventId()));
//        }
//
//        //TODO add data
//        public static EventRecord indexFlushed(long logPosition, long timeTaken, int numEntries) {
//
//            return EventRecord.create(INDEX_STREAM, INDEX_FLUSHED_TYPE, "");
//        }
//
////        //TODO add data
////        public static EventRecord createSegmentRecord(String type, EventRecord event) {
////            return EventRecord.create(SEGMENT_STREAM, type, StringUtils.toUtf8Bytes(event.eventId()));
////        }
//
//        public static EventRecord streamDeletedRecord(long streamHash) {
//            byte[] data = ByteBuffer.allocate(Long.BYTES).putLong(streamHash).array();
//            return EventRecord.create(STREAMS_STREAM, STREAM_DELETED_TYPE, data);
//        }
//
//        public static EventRecord streamCreatedRecord(StreamMetadata metadata) {
//            byte[] data = streamSerializer.toBytes(metadata).array();
//            return EventRecord.create(STREAMS_STREAM, STREAM_CREATED_TYPE, data);
//        }
//
//        //TODO add data
//        public static EventRecord createProjectionsRecord(String type, EventRecord event) {
//            return EventRecord.create(PROJECTIONS_STREAM, type, StringUtils.toUtf8Bytes(event.eventId()));
//        }

    }

}
