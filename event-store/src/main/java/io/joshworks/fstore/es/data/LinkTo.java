package io.joshworks.fstore.es.data;

import io.joshworks.fstore.es.log.EventRecord;
import io.joshworks.fstore.es.utils.StringUtils;

import static io.joshworks.fstore.es.data.Constant.SYSTEM_PREFIX;

public class LinkTo {

    public static final String TYPE = SYSTEM_PREFIX + ">";

    public final String stream;
    public final int version;

    public LinkTo(String stream, int version) {
        this.stream = stream;
        this.version = version;
    }

    public static EventRecord create(String stream, EventRecord event) {
        return EventRecord.create(stream, TYPE, StringUtils.toUtf8Bytes(event.eventId()));
    }

    public static LinkTo from(EventRecord record) {
        String[] split = record.dataAsString().split(EventRecord.STREAM_VERSION_SEPARATOR);
        return new LinkTo(split[0], Integer.parseInt(split[1]));
    }

}
