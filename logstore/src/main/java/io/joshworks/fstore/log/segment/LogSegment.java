package io.joshworks.fstore.log.segment;


import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Checksum;
import io.joshworks.fstore.log.LogIterator;
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

    private final Serializer<Header> headerSerializer = new HeaderSerializer();
    private final Serializer<T> serializer;
    private final Storage storage;
    private final DataReader reader;

    private int entries;

    private Header header;

    public LogSegment(Storage storage, Serializer<T> serializer, DataReader reader, long position) {
        this(storage, serializer, reader, position, null); //when type is null,
    }

    //Type is only used for new segments, accepted values are Type.LOG_HEAD or Type.MERGE_OUT
    public LogSegment(Storage storage, Serializer<T> serializer, DataReader reader, long position, Type type) {
        this.serializer = serializer;
        this.storage = storage;
        this.reader = reader;
        this.header = readHeader(storage, type);//must come before position(position)
        this.entries = header.entries;

        if (Type.LOG_HEAD.equals(header.type) && position > 0) {
            SegmentState result = rebuildState(position);
            this.position(result.position);
            this.entries = result.entries;
        }
    }

    private Header readHeader(Storage storage, Type type) {
        ByteBuffer bb = ByteBuffer.allocate(Header.SIZE);
        storage.read(0, bb);
        Header header = headerSerializer.fromBytes(bb);
        if (Header.EMPTY.equals(header)) {
            //new segment, create a header
            if (type == null) {
                throw new IllegalArgumentException("Type must provided when creating a new segment");
            }
            if (!Type.LOG_HEAD.equals(type) && !Type.MERGE_OUT.equals(type)) {
                throw new IllegalArgumentException("Only Type.LOG_HEAD and Type.MERGE_OUT are accepted when creating a segment");
            }

            return new Header(0, System.currentTimeMillis(), 0, type);
        }
        return header;
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

        entries++;
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
    public SegmentState rebuildState(long lastKnownPosition) {
        long position = lastKnownPosition;
        int foundEntries = 0;
        try {
            logger.info("Restoring log state and checking consistency from position {}", lastKnownPosition);
            LogIterator<T> logIterator = iterator(lastKnownPosition);
            while (logIterator.hasNext()) {
                logIterator.next();
                foundEntries++;
                position = logIterator.position();
            }
        } catch (Exception e) {
            logger.warn("Found inconsistent entry on position {}, segment '{}'", position, name());
        }
        logger.info("Log state restored, current position {}", position);
        return new SegmentState(foundEntries, position);
    }

    @Override
    public void delete() {
        storage.delete();
    }

    @Override
    public void roll(int level) {
        if (Type.READ_ONLY.equals(header.type)) {
            throw new IllegalStateException("Cannot roll readOnly segment");
        }
        storage.shrink();

        this.header = new Header(entries, header.created, level, Type.READ_ONLY);
        storage.position(0);
        ByteBuffer headerData = headerSerializer.toBytes(this.header);
        storage.write(headerData);
    }

    @Override
    public boolean readOnly() {
        return Type.READ_ONLY.equals(header.type);
    }

    @Override
    public int entries() {
        return entries;
    }

    @Override
    public int level() {
        return header.level;
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
        private long readAheadPosition;
        private long lastReadSize;

        private LogReader(Storage storage, DataReader reader, Serializer<T> serializer) {
            this(storage, reader, serializer, 0);
        }

        private LogReader(Storage storage, DataReader reader, Serializer<T> serializer, long initialPosition) {
            this.storage = storage;
            this.reader = reader;
            this.serializer = serializer;
            this.position = initialPosition;
            this.readAheadPosition = initialPosition;
            this.data = readAhead();
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
            if (data == null) {
                throw new NoSuchElementException();
            }
            T current = data;
            position += lastReadSize;
            data = readAhead();
            return current;
        }

        private T readAhead() {
            ByteBuffer bb = reader.read(storage, readAheadPosition);
            if (bb.remaining() == 0) { //EOF
                return null;
            }
            lastReadSize = bb.limit();
            readAheadPosition += bb.limit();
            return serializer.fromBytes(bb);
        }
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("LogSegment{");
        sb.append(", handler=").append(storage.name());
        sb.append(", entries=").append(entries);
        sb.append(", header=").append(header);
        sb.append('}');
        return sb.toString();
    }
}
