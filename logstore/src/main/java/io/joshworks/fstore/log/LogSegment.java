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
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LogSegment<T> implements Log<T> {

    private static final Logger logger = LoggerFactory.getLogger(LogSegment.class);

    private final Serializer<T> serializer;
    private final Storage storage;
    private final DataReader reader;

    public static <T> Log<T> create(Storage storage, Serializer<T> serializer, DataReader reader) {
        return new LogSegment<>(storage, serializer, reader);
    }

    public static <T> Log<T> open(Storage storage, Serializer<T> serializer, DataReader reader, long position) {
        LogSegment<T> appender = null;
        try {

            appender = new LogSegment<>(storage, serializer, reader);
            appender.position(position);
            return appender;
        } catch (CorruptedLogException e) {
            IOUtils.closeQuietly(appender);
            throw e;
        }
    }

    private LogSegment(Storage storage, Serializer<T> serializer, DataReader reader) {
        this.serializer = serializer;
        this.storage = storage;
        this.reader = reader;
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
    public Scanner<T> scanner() {
        return new LogReader<>(storage, reader, serializer);
    }

    @Override
    public Stream<T> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(scanner(), Spliterator.ORDERED), false);
    }

    @Override
    public Scanner<T> scanner(long position) {
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
            Scanner<T> scanner = scanner(lastKnownPosition);
            while (scanner.hasNext()) {
                T next = scanner.next();
                if (next == null) {
                    logger.warn("Found inconsistent entry on position {}, segment '{}'", position, name());
                    break;
                }
                position = scanner.position();
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
    public Log<T> seal() {
        storage.shrink();
        return new ReadOnlySegment<>(this);
    }


    //NOT THREAD SAFE
    private static class LogReader<T> extends Scanner<T> {

        private LogReader(Storage storage, DataReader reader, Serializer<T> serializer) {
            this(storage, reader, serializer, 0);
        }

        private LogReader(Storage storage, DataReader reader, Serializer<T> serializer, long initialPosition) {
            super(storage, reader, serializer, initialPosition);
        }

        @Override
        protected T readAndVerify() {
            long currentPos = position;

            ByteBuffer data = reader.read(storage, currentPos);
            if (data.remaining() == 0) { //EOF
                return null;
            }
            position += data.limit();
            return serializer.fromBytes(data);
        }

        @Override
        public long position() {
            return position;
        }

    }

    static long write(Storage storage, ByteBuffer bytes) {
        ByteBuffer bb = ByteBuffer.allocate(ENTRY_HEADER_SIZE + bytes.remaining());
        bb.putInt(bytes.remaining());
        bb.putInt(Checksum.crc32(bytes));
        bb.put(bytes);

        bb.flip();
        return storage.write(bb);
    }
}
