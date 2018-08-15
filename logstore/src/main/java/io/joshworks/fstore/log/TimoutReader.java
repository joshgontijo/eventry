package io.joshworks.fstore.log;

public abstract class TimoutReader {
    protected long lastReadTs;

    public long lastReadTs() {
        return lastReadTs;
    }
}
