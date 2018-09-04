package io.joshworks.fstore.es.log;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.es.stream.StreamMetadata;
import io.joshworks.fstore.es.stream.StreamMetadataSerializer;
import io.joshworks.fstore.es.utils.StringUtils;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
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
        return type.startsWith(System.PREFIX);
    }

    public boolean isLinkToEvent() {
        return System.LINKTO_TYPE.equals(type);
    }


    public static class System  {

        private static final Serializer<StreamMetadata> streamSerializer = new StreamMetadataSerializer();

        private static final String PREFIX = "_";

        //system streams
//        private static final String SEGMENT_STREAM = PREFIX + "segments";
        public static final String INDEX_STREAM = PREFIX + "index";
        public static final String STREAMS_STREAM = PREFIX + "streams";
        public static final String PROJECTIONS_STREAM = PREFIX + "projections";

        //system types
        private static final String LINKTO_TYPE = PREFIX + ">";

        private static final String INDEX_FLUSHED_TYPE = PREFIX + "INDEX_FLUSHED";

//        private static final String SEGMENT_ROLLED_TYPE = PREFIX + "SEGMENT_ROLLED";
        public static final String STREAM_CREATED_TYPE = PREFIX + "STREAM_CREATED";
        public static final String STREAM_DELETED_TYPE = PREFIX + "STREAM_DELETED";
        public static final String STREAM_UPDATED_TYPE = PREFIX + "STREAM_UPDATED";
        public static final String PROJECTION_CREATED_TYPE = PREFIX + "PROJECTION_CREATED";
        public static final String PROJECTION_UPDATED_TYPE = PREFIX + "PROJECTION_UPDATED";
        public static final String PROJECTION_DELETED_TYPE = PREFIX + "PROJECTION_DELETED";
        public static final String PROJECTION_COMPLETED_TYPE = PREFIX + "PROJECTION_COMPLETED";
        public static final String PROJECTION_STARTED_TYPE = PREFIX + "PROJECTION_STARTED";

        public static EventRecord createLinkTo(String stream, EventRecord event) {
            return EventRecord.create(stream, LINKTO_TYPE, StringUtils.toUtf8Bytes(event.eventId()));
        }

        //TODO add data
        public static EventRecord createIndexFlushed() {
            return EventRecord.create(INDEX_STREAM, INDEX_FLUSHED_TYPE, "");
        }

//        //TODO add data
//        public static EventRecord createSegmentRecord(String type, EventRecord event) {
//            return EventRecord.create(SEGMENT_STREAM, type, StringUtils.toUtf8Bytes(event.eventId()));
//        }

        public static EventRecord streamDeletedRecord(long streamHash) {
            byte[] data = ByteBuffer.allocate(Long.BYTES).putLong(streamHash).array();
            return EventRecord.create(STREAMS_STREAM, STREAM_DELETED_TYPE, data);
        }

        public static EventRecord streamCreatedRecord(StreamMetadata metadata) {
            byte[] data = streamSerializer.toBytes(metadata).array();
            return EventRecord.create(STREAMS_STREAM, STREAM_CREATED_TYPE, data);
        }

        //TODO add data
        public static EventRecord createProjectionsRecord(String type, EventRecord event) {
            return EventRecord.create(PROJECTIONS_STREAM, type, StringUtils.toUtf8Bytes(event.eventId()));
        }

    }

}
