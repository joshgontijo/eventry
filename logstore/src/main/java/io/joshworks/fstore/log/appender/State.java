package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.LogFileUtils;
import io.joshworks.fstore.log.segment.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class State implements Closeable {

    private static final int SIZE = 4096;

    private static final Logger logger = LoggerFactory.getLogger(State.class);

    private final Storage storage;

    private long position;
    private long entryCount;
    private long lastRollTime;

    private boolean dirty;

    private State(Storage storage, long position, long entryCount, long lastRollTime) {
        this.storage = storage;
        this.position = position;
        this.entryCount = entryCount;
        this.lastRollTime = lastRollTime;
    }

    private static Storage createStorage(File directory) {
        return new RafStorage(new File(directory, LogFileUtils.STATE_FILE), SIZE, Mode.READ_WRITE);
    }

    public void position(long position) {
        this.position = position;
        this.dirty = true;
    }

    public void incrementEntryCount() {
        this.entryCount++;
        this.dirty = true;
    }

    public void lastRollTime(long lastRollTime) {
        this.lastRollTime = lastRollTime;
        this.dirty = true;
    }

    public long position() {
        return position;
    }

    public long entryCount() {
        return entryCount;
    }

    public long lastRollTime() {
        return lastRollTime;
    }

    public static State readFrom(File directory) {
        Storage storage = null;
        try {
            storage = createStorage(directory);
            ByteBuffer data = ByteBuffer.allocate(74);
            storage.read(0, data);

            data.flip();

            int length = data.getInt();
            if (length > data.capacity()) {
                data = ByteBuffer.allocate(length);
                storage.read(0, data);
                data.getInt(); //ignore
            }

            long lastPosition = data.getLong();
            long entryCount = data.getLong();
            long lastRollTime = data.getLong();

            State state = new State(storage, lastPosition, entryCount, lastRollTime);
            logger.info("Reading state {}", state);
            return state;

        } catch (Exception e) {
            IOUtils.closeQuietly(storage);
            throw e;
        }
    }

    public static State empty(File directory) {
        Storage storage = createStorage(directory);
        return new State(storage, Log.START, 0L, System.currentTimeMillis());
    }

    public synchronized void flush() {
        if (!dirty) {
            return;
        }
        logger.info("Writing state {}", this);

        int length = Integer.BYTES + (Long.BYTES * 3);
        ByteBuffer bb = ByteBuffer.allocate(length);
        bb.putInt(length);
        bb.putLong(position);
        bb.putLong(entryCount);
        bb.putLong(lastRollTime);

        bb.flip();

        write(bb);

    }

    private void write(ByteBuffer data) {
        try {
            storage.position(0);
            storage.write(data);
            storage.flush();
            dirty = false;
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    @Override
    public String toString() {
        return "{position=" + position +
                ", entryCount=" + entryCount +
                ", lastRollTime=" + lastRollTime +
                '}';
    }

    @Override
    public void close() {
        try {
            flush();
            logger.info("Closing state file handler");
            storage.close();
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }
}
