package io.joshworks.fstore.es.data;

import static io.joshworks.fstore.es.data.Constant.SYSTEM_PREFIX;

public class SystemTypes {

    public static final String LINKTO_TYPE = SYSTEM_PREFIX + ">";

    public static final String INDEX_FLUSHED_TYPE = SYSTEM_PREFIX + "INDEX_FLUSHED";

            private static final String SEGMENT_ROLLED_TYPE = SYSTEM_PREFIX + "SEGMENT_ROLLED";

    public static final String STREAM_DELETED_TYPE = SYSTEM_PREFIX + "STREAM_DELETED";
    public static final String STREAM_UPDATED_TYPE = SYSTEM_PREFIX + "STREAM_UPDATED";
    public static final String PROJECTION_CREATED_TYPE = SYSTEM_PREFIX + "PROJECTION_CREATED";
    public static final String PROJECTION_UPDATED_TYPE = SYSTEM_PREFIX + "PROJECTION_UPDATED";
    public static final String PROJECTION_DELETED_TYPE = SYSTEM_PREFIX + "PROJECTION_DELETED";
    public static final String PROJECTION_COMPLETED_TYPE = SYSTEM_PREFIX + "PROJECTION_COMPLETED";
    public static final String PROJECTION_STARTED_TYPE = SYSTEM_PREFIX + "PROJECTION_STARTED";

}
