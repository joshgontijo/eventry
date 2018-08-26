package io.joshworks.fstore.log;

import java.util.UUID;

public abstract class TimeoutReader {

    protected final String uuid = UUID.randomUUID().toString().substring(0,8);
    protected long lastReadTs;

    public long lastReadTs() {
        return lastReadTs;
    }
}
