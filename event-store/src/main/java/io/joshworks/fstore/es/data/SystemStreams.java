package io.joshworks.fstore.es.data;

import static io.joshworks.fstore.es.data.Constant.SYSTEM_PREFIX;

public class SystemStreams {

    private static final String SEGMENTS = SYSTEM_PREFIX + "segments";
    public static final String INDEX = SYSTEM_PREFIX + "index";
    public static final String STREAMS = SYSTEM_PREFIX + "streams";
    public static final String PROJECTIONS = SYSTEM_PREFIX + "projections";

}
