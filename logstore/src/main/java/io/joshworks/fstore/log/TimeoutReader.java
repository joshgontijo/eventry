package io.joshworks.fstore.log;

public abstract class TimeoutReader {

    protected long lastReadTs;

    public long lastReadTs() {
        return lastReadTs;
    }
}
