package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.LogFileUtils;
import io.joshworks.fstore.serializer.Serializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class State implements Closeable {

    private static final int SIZE = 4096;
    private static final Serializer<String> stringSerializer = Serializers.VSTRING;
    private static final String LEVEL_SEPARATOR = ",";

    private static final Logger logger = LoggerFactory.getLogger(State.class);

    private final Storage storage;

    private long position;
    private long entryCount;
    private long lastRollTime;
    private List<List<String>> levels;

    private boolean dirty;

    private State(Storage storage, long position, long entryCount, long lastRollTime, List<List<String>> levels) {
        this.storage = storage;
        this.position = position;
        this.entryCount = entryCount;
        this.lastRollTime = lastRollTime;
        this.levels = levels;
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

    public void levels(List<List<String>> levels) {
        this.levels = levels;
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

    public List<List<String>> levels() {
        return levels;
    }

    public static State readFrom(File directory) {
        Storage storage = createStorage(directory);
        try {
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

            List<List<String>> levels = new LinkedList<>();
            while (data.position() < length) {
                String level = stringSerializer.fromBytes(data);
                if (level == null || level.isEmpty()) {
                    continue;
                }
                String[] split = level.split(LEVEL_SEPARATOR);

                LinkedList<String> levelSegments = Stream.of(split)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toCollection(LinkedList::new));
                if (!levelSegments.isEmpty()) {
                    levels.add(levelSegments);
                }

            }
            State state = new State(storage, lastPosition, entryCount, lastRollTime, levels);
            logger.info("Reading state {}", state);
            return state;

        } catch (Exception e) {
            IOUtils.closeQuietly(storage);
            throw e;
        }
    }

    public static State empty(File directory) {
        Storage storage = createStorage(directory);
        return new State(storage, 0L, 0L, System.currentTimeMillis(), new LinkedList<>());
    }

    public void flush() {
        if (!dirty) {
            return;
        }
        logger.info("Writing state {}", toString());
        List<ByteBuffer> segmentsData = new ArrayList<>();
        for (List<String> level : levels) {
            StringJoiner joiner = new StringJoiner(LEVEL_SEPARATOR);
            for (String segmentName : level) {
                joiner.add(segmentName);
            }
            segmentsData.add(stringSerializer.toBytes(joiner.toString()));
        }

        int segmentsNameDataSize = segmentsData.stream().mapToInt(ByteBuffer::limit).sum();

        int length = Integer.BYTES + (Long.BYTES * 3) + segmentsNameDataSize;
        ByteBuffer bb = ByteBuffer.allocate(length);
        bb.putInt(length);
        bb.putLong(position);
        bb.putLong(entryCount);
        bb.putLong(lastRollTime);
        for (ByteBuffer segmentData : segmentsData) {
            bb.put(segmentData);
        }

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
                ", segments=" + levels +
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
