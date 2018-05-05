package io.joshworks.fstore.log;


import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LogSegment<T> implements Log<T> {

    private static final Logger logger = LoggerFactory.getLogger(LogSegment.class);

    private final Serializer<T> serializer;
    private final Storage storage;
    private final DataReader reader;
    private final boolean readOnly;

    public LogSegment(Storage storage, Serializer<T> serializer, DataReader reader, long position, boolean readOnly) {
        this.serializer = serializer;
        this.storage = storage;
        this.reader = reader;
        this.position(position);
        this.readOnly = readOnly;
    }

    private void position(long position) {
        this.storage.position(position);
    }

    @Override
    public long position() {
        return storage.position();
    }

    @Override
    public T get(long position) {
        ByteBuffer data = reader.read(storage, position);
        if (data.remaining() == 0) { //EOF
            return null;
        }
        return serializer.fromBytes(data);
    }

    @Override
    public long size() {
        return storage.position();
    }

    @Override
    public long append(T data) {
        ByteBuffer bytes = serializer.toBytes(data);

        long recordPosition = position();
        write(storage, bytes);

        return recordPosition;
    }

    @Override
    public String name() {
        return storage.name();
    }

    @Override
    public LogIterator<T> iterator() {
        return new LogReader<>(storage, reader, serializer);
    }

    @Override
    public Stream<T> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED), false);
    }

    @Override
    public LogIterator<T> iterator(long position) {
        return new LogReader<>(storage, reader, serializer, position);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(storage);
    }

    @Override
    public void flush() {
        try {
            storage.flush();
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    @Override
    public long checkIntegrity(long lastKnownPosition) {
        long position = lastKnownPosition;
        try {
            logger.info("Restoring log state and checking consistency from position {}", lastKnownPosition);
            LogIterator<T> logIterator = iterator(lastKnownPosition);
            while (logIterator.hasNext()) {
                T next = logIterator.next();
                if (next == null) {
                    logger.warn("Found inconsistent entry on position {}, segment '{}'", position, name());
                    break;
                }
                position = logIterator.position();
            }
        } catch (Exception e) {
            logger.warn("Found inconsistent entry on position {}, segment '{}'", position, name());
            return position;
        }
        logger.info("Log state restored, current position {}", position);
        return position;
    }

    @Override
    public void delete() {
        storage.delete();
    }

    @Override
    public void roll() {
        storage.shrink();
    }

    @Override
    public boolean readOnly() {
        return readOnly;
    }

    static long write(Storage storage, ByteBuffer bytes) {
        ByteBuffer bb = ByteBuffer.allocate(ENTRY_HEADER_SIZE + bytes.remaining());
        bb.putInt(bytes.remaining());
        bb.putInt(Checksum.crc32(bytes));
        bb.put(bytes);

        bb.flip();
        return storage.write(bb);
    }

    //NOT THREAD SAFE
    private static class LogReader<T> implements LogIterator<T> {

        private final Storage storage;
        private final DataReader reader;
        private final Serializer<T> serializer;

        private T data;
        protected long position;

        private LogReader(Storage storage, DataReader reader, Serializer<T> serializer) {
            this(storage, reader, serializer, 0);
        }

        private LogReader(Storage storage, DataReader reader, Serializer<T> serializer, long initialPosition) {
            this.storage = storage;
            this.reader = reader;
            this.serializer = serializer;
            position = initialPosition;
            this.data = readData();
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public boolean hasNext() {
            return data != null;
        }

        @Override
        public T next() {
            if(data == null) {
                throw new NoSuchElementException();
            }
            T current = data;
            data = readData();
            return current;
        }

        private T readData() {
            long currentPos = position;

            ByteBuffer bb = reader.read(storage, currentPos);
            if (bb.remaining() == 0) { //EOF
                return null;
            }
            position += bb.limit();
            return serializer.fromBytes(bb);
        }
    }
}
