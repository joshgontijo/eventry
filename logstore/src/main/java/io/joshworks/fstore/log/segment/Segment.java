package io.joshworks.fstore.log.segment;


import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Checksum;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.TimeoutReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

/**
 * File format:
 * <p>
 * |---- HEADER ----|----- LOG -----|--- END OF LOG (8bytes) ---|---- FOOTER ----|
 */
public class Segment<T> implements Log<T> {

    private static final Logger logger = LoggerFactory.getLogger(Segment.class);

    private final Serializer<Header> headerSerializer = new HeaderSerializer();
    private final Serializer<T> serializer;
    private final Storage storage;
    private final DataReader reader;
    private final String magic;

    private long entries;
    private final AtomicBoolean closed = new AtomicBoolean();

    private Header header;

    private final Set<TimeoutReader> readers = ConcurrentHashMap.newKeySet();

    public Segment(Storage storage, Serializer<T> serializer, DataReader reader, String magic) {
        this(storage, serializer, reader, magic, null);
    }

    //Type is only used for new segments, accepted values are Type.LOG_HEAD or Type.MERGE_OUT
    //Magic is used to create new segment or verify existing
    public Segment(Storage storage, Serializer<T> serializer, DataReader reader, String magic, Type type) {
        this.serializer = requireNonNull(serializer, "Serializer must be provided");
        this.storage = requireNonNull(storage, "Storage must be provided");
        this.reader = requireNonNull(reader, "Reader must be provided");
        this.magic = requireNonNull(magic, "Magic must be provided");

        Header readHeader = readHeader(storage);

        if (Header.EMPTY.equals(readHeader)) { //new segment
            if (type == null) {
                IOUtils.closeQuietly(storage);
                throw new SegmentException("Segment type must be provided when creating a new segment");
            }
            this.header = createNewHeader(storage, type, magic);
            this.position(Log.START);
            return;
        }
        this.header = readHeader;

        byte[] expected = header.magic.getBytes(StandardCharsets.UTF_8);
        byte[] actual = magic.getBytes(StandardCharsets.UTF_8);

        if (!Arrays.equals(expected, actual)) {
            IOUtils.closeQuietly(storage);
            throw new InvalidMagic(header.magic, magic);
        }
        this.position(Header.BYTES);

        this.entries = header.entries;
        if (Type.LOG_HEAD.equals(header.type)) { //reopening log head
            SegmentState result = rebuildState(Segment.START);
            this.position(result.position);
            this.entries = result.entries;
        }
    }

    private Header readHeader(Storage storage) {
        ByteBuffer bb = ByteBuffer.allocate(Header.BYTES);
        storage.read(0, bb);
        bb.flip();
        if (bb.remaining() == 0) {
            return Header.EMPTY;
        }
        return headerSerializer.fromBytes(bb);

    }

    private Header createNewHeader(Storage storage, Type type, String magic) {
        validateTypeProvided(type);
        Header newHeader = new Header(magic, 0, System.currentTimeMillis(), 0, type, 0, 0, 0, 0, 0);
        ByteBuffer headerData = headerSerializer.toBytes(newHeader);
        if (storage.write(headerData) != Header.BYTES) {
            throw new SegmentException("Failed to create header");
        }
        try {
            storage.flush();
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
        return newHeader;
    }

    private void validateTypeProvided(Type type) {
        //new segment, create a header
        if (type == null) {
            throw new IllegalArgumentException("Type must provided when creating a new segment");
        }
        if (!Type.LOG_HEAD.equals(type) && !Type.MERGE_OUT.equals(type)) {
            throw new IllegalArgumentException("Only Type.LOG_HEAD and Type.MERGE_OUT are accepted when creating a segment");
        }
    }

    private void position(long position) {
        if (position < START) {
            throw new IllegalArgumentException("Position must be at least " + Header.BYTES);
        }
        this.storage.position(position);
    }

    @Override
    public long position() {
        return storage.position();
    }

    @Override
    public Marker marker() {
        if (readOnly()) {
            return new Marker(header.logStart, header.logEnd, header.footerStart, header.footerEnd);
        }
        return new Marker(header.logStart, -1, -1, -1);
    }

    @Override
    public T get(long position) {
        checkBounds(position);
        ByteBuffer data = reader.readForward(storage, position);
        if (data.remaining() == 0) { //EOF
            return null;
        }
        return serializer.fromBytes(data);
    }

    @Override
    public PollingSubscriber<T> poller(long position) {
        SegmentPoller segmentPoller = new SegmentPoller(storage, reader, serializer, position);
        return addToReaders(segmentPoller);
    }

    @Override
    public PollingSubscriber<T> poller() {
        return poller(Segment.START);
    }

    private void checkBounds(long position) {
        if (position < START) {
            throw new IllegalArgumentException("Position must be greater or equals to " + START);
        }
        if (readOnly() && position > header.logEnd) {
            throw new IllegalArgumentException("Position must be less than " + header.logEnd);
        }
    }

    @Override
    public long size() {
        return storage.position();
    }

    @Override
    public Set<TimeoutReader> readers() {
        return readers;
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
    public Stream<T> stream(Direction direction) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(direction), Spliterator.ORDERED), false);
    }

    @Override
    public LogIterator<T> iterator(Direction direction) {
        if (Direction.FORWARD.equals(direction)) {
            return iterator(Log.START, direction);
        }
        if(readOnly()) {
            return iterator(header.logEnd, direction);
        }
        return iterator(position(), direction);

    }

    @Override
    public LogIterator<T> iterator(long position, Direction direction) {
        return newLogReader(position, direction);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
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
        if (lastKnownPosition < START) {
            throw new IllegalStateException("Invalid lastKnownPosition: " + lastKnownPosition + ",value must be at least " + START);
        }
        long position = lastKnownPosition;
        int foundEntries = 0;
        long start = System.currentTimeMillis();
        try {
            logger.info("Restoring log state and checking consistency from position {}", lastKnownPosition);
            LogIterator<T> logIterator = iterator(lastKnownPosition, Direction.FORWARD);
            while (logIterator.hasNext()) {
                logIterator.next();
                foundEntries++;
                position = logIterator.position();
            }
        } catch (Exception e) {
            logger.warn("Found inconsistent entry on position {}, segment '{}': {}", position, name(), e.getMessage());
        }
        logger.info("Log state restored in {}ms, current position: {}, entries: {}", start, position, foundEntries);
        if (position < Header.BYTES) {
            throw new IllegalStateException("Initial log state position must be at least " + Header.BYTES);
        }
        return new SegmentState(foundEntries, position);
    }

    @Override
    public void delete() {
        storage.delete();
    }

    @Override
    public void roll(int level) {
        roll(level, null);
    }

    @Override
    public void roll(int level, ByteBuffer footer) {
        if (Type.READ_ONLY.equals(header.type)) {
            throw new IllegalStateException("Cannot roll read only segment");
        }

        writeEndOfLog();
        long endOfLog = storage.position();
        FooterInfo footerInfo = footer != null ? writeFooter(footer) : new FooterInfo(endOfLog, 0);
        this.header = writeHeader(level, footerInfo);

        boolean hasFooter = header.footerStart > 0;
        long endOfSegment = hasFooter ? header.footerStart + header.footerEnd : endOfLog;
        storage.truncate(endOfSegment);
    }

    @Override
    public ByteBuffer readFooter() {
        if (!readOnly()) {
            throw new IllegalStateException("Segment is not read only");
        }
        if (header.footerStart <= 0 || header.footerEnd <= 0) {
            return ByteBuffer.allocate(0);
        }

        ByteBuffer footer = ByteBuffer.allocate((int) (header.footerEnd - header.footerStart));
        storage.read(header.footerStart, footer);
        footer.flip();
        return footer;
    }

    private <R extends TimeoutReader> R addToReaders(R reader) {
        readers.add(reader);
        return reader;
    }

    private <R extends TimeoutReader> void removeFromReaders(R reader) {
        readers.remove(reader);
    }

    private void writeEndOfLog() {
        storage.write(ByteBuffer.wrap(Log.EOL));
    }

    private FooterInfo writeFooter(ByteBuffer footer) {
        long pos = storage.position();
        int size = footer.remaining();
        storage.write(footer);
        return new FooterInfo(pos, pos + size);
    }

    private Header writeHeader(int level, FooterInfo footerInfo) {
        long segmentSize = footerInfo.start + footerInfo.end;
        long logEnd = footerInfo.start - EOL.length;
        Header newHeader = new Header(this.magic, entries, this.header.created, level, Type.READ_ONLY, segmentSize, Log.START, logEnd, footerInfo.start, footerInfo.end);
        storage.position(0);
        ByteBuffer headerData = headerSerializer.toBytes(newHeader);
        storage.write(headerData);
        return newHeader;
    }

    //TODO properly implement reader pool
    //TODO implement race condition on acquiring readers and closing / deleting segment
    protected LogReader newLogReader(long pos, Direction direction) {

        while (readers.size() >= 10) {
            try {
                Thread.sleep(1000);
                logger.info("Waiting to acquire reader");
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }

        LogReader logReader = new LogReader(storage, reader, serializer, pos, direction);
        return addToReaders(logReader);
    }

    @Override
    public boolean readOnly() {
        return Type.READ_ONLY.equals(header.type);
    }

    @Override
    public long entries() {
        return entries;
    }

    @Override
    public int level() {
        return header.level;
    }

    @Override
    public long created() {
        return header.created;
    }

    static long write(Storage storage, ByteBuffer bytes) {
        int entrySize = bytes.remaining();
        ByteBuffer bb = ByteBuffer.allocate(HEADER_OVERHEAD + entrySize);
        bb.putInt(entrySize);
        bb.putInt(Checksum.crc32(bytes));
        bb.put(bytes);
        bb.putInt(entrySize);

        bb.flip();
        return storage.write(bb);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Segment<?> that = (Segment<?>) o;
        return entries == that.entries &&
                Objects.equals(headerSerializer, that.headerSerializer) &&
                Objects.equals(serializer, that.serializer) &&
                Objects.equals(storage, that.storage) &&
                Objects.equals(reader, that.reader) &&
                Objects.equals(header, that.header);
    }

    @Override
    public int hashCode() {

        return Objects.hash(headerSerializer, serializer, storage, reader, entries, header);
    }

    @Override
    public String toString() {
        return "LogSegment{" + "handler=" + storage.name() +
                ", entries=" + entries +
                ", header=" + header +
                ", readers=" + Arrays.toString(readers.toArray()) +
                '}';
    }

    private static class FooterInfo {

        private final long start;
        private final long end;

        private FooterInfo(long start, long end) {
            this.start = start;
            this.end = end;
        }
    }

    //NOT THREAD SAFE
    private class LogReader extends TimeoutReader implements LogIterator<T> {

        private final Storage storage;
        private final DataReader reader;
        private final Serializer<T> serializer;

        private T data;
        protected long position;
        private long readAheadPosition;
        private int lastReadSize;
        private final Direction direction;

        LogReader(Storage storage, DataReader reader, Serializer<T> serializer, long initialPosition, Direction direction) {
            this.direction = direction;
            checkBounds(initialPosition);
            this.storage = storage;
            this.reader = reader;
            this.serializer = serializer;
            this.position = initialPosition;
            this.readAheadPosition = initialPosition;
            this.data = readAhead();
            this.lastReadTs = System.currentTimeMillis();
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
                close();
                throw new NoSuchElementException();
            }
            lastReadTs = System.currentTimeMillis();

            T current = data;
            position = Direction.FORWARD.equals(direction) ? position + lastReadSize : position - lastReadSize;
            data = readAhead();
            return current;
        }

        private T readAhead() {
            ByteBuffer bb = Direction.FORWARD.equals(direction) ? reader.readForward(storage, readAheadPosition) : reader.readBackward(storage, readAheadPosition);
            if (bb.remaining() == 0) { //EOF
                close();
                return null;
            }
            lastReadSize = bb.remaining() + Log.HEADER_OVERHEAD;
            readAheadPosition = Direction.FORWARD.equals(direction) ? readAheadPosition + lastReadSize : readAheadPosition - lastReadSize;
            return serializer.fromBytes(bb);
        }

        @Override
        public void close() {
            Segment.this.removeFromReaders(this);
        }

        @Override
        public String toString() {
            return "SegmentPoller{ uuid='" + uuid + '\'' +
                    ", readPosition=" + position +
                    ", order=" + direction +
                    ", readAheadPosition=" + readAheadPosition +
                    ", lastReadTs=" + lastReadTs +
                    '}';
        }
    }

    private class SegmentPoller extends TimeoutReader implements PollingSubscriber<T> {

        private static final int VERIFICATION_INTERVAL_MILLIS = 500;

        private final Storage storage;
        private final DataReader reader;
        private final Serializer<T> serializer;
        private long readPosition;

        SegmentPoller(Storage storage, DataReader reader, Serializer<T> serializer, long initialPosition) {
            checkBounds(initialPosition);
            this.storage = storage;
            this.reader = reader;
            this.serializer = serializer;
            this.readPosition = initialPosition;
            this.lastReadTs = System.currentTimeMillis();
        }

        private T read(boolean advance) {
            ByteBuffer bb = reader.readForward(storage, readPosition);
            if (bb.remaining() == 0) { //EOF
                close();
                return null;
            }
            if (advance) {
                readPosition += bb.remaining() + Log.HEADER_OVERHEAD;
            }
            return serializer.fromBytes(bb);
        }

        private synchronized T tryTake(long sleepInterval, TimeUnit timeUnit, boolean advance) throws InterruptedException {
            if (hasDataAvailable()) {
                this.lastReadTs = System.currentTimeMillis();
                return read(true);
            }
            waitForData(sleepInterval, timeUnit);
            this.lastReadTs = System.currentTimeMillis();
            return read(advance);
        }

        private boolean hasDataAvailable() {
            if (Segment.this.readOnly()) {
                return readPosition < Segment.this.header.logEnd;
            }
            return readPosition < Segment.this.position();
        }

        private synchronized T tryPool(long time, TimeUnit timeUnit) throws InterruptedException {
            if (hasDataAvailable()) {
                this.lastReadTs = System.currentTimeMillis();
                return read(true);
            }
            if (time > 0) {
                waitFor(time, timeUnit);
            }
            this.lastReadTs = System.currentTimeMillis();
            return read(true);
        }

        private void waitFor(long time, TimeUnit timeUnit) throws InterruptedException {
            long elapsed = 0;
            long start = System.currentTimeMillis();
            long maxWaitTime = timeUnit.toMillis(time);
            long interval = Math.min(maxWaitTime, VERIFICATION_INTERVAL_MILLIS);
            while (!closed.get() && !hasDataAvailable() && elapsed < maxWaitTime) {
                TimeUnit.MILLISECONDS.sleep(interval);
                elapsed = System.currentTimeMillis() - start;
            }
        }

        private void waitForData(long time, TimeUnit timeUnit) throws InterruptedException {
            while (!closed.get() && !readOnly() && !hasDataAvailable()) {
                timeUnit.sleep(time);
                this.lastReadTs = System.currentTimeMillis();
            }
        }

        @Override
        public T peek() throws InterruptedException {
            return tryTake(VERIFICATION_INTERVAL_MILLIS, TimeUnit.MILLISECONDS, false);
        }

        @Override
        public T poll() throws InterruptedException {
            return tryPool(NO_SLEEP, TimeUnit.MILLISECONDS);
        }

        @Override
        public T poll(long time, TimeUnit timeUnit) throws InterruptedException {
            return tryPool(time, timeUnit);
        }

        @Override
        public T take() throws InterruptedException {
            return tryTake(VERIFICATION_INTERVAL_MILLIS, TimeUnit.MILLISECONDS, true);
        }

        @Override
        public boolean headOfLog() {
            if (Segment.this.readOnly()) {
                return readPosition >= Segment.this.header.logEnd;
            }
            return readPosition == Segment.this.position();
        }

        @Override
        public boolean endOfLog() {
            return readOnly() && readPosition >= header.logEnd;

        }

        @Override
        public long position() {
            return readPosition;
        }

        @Override
        public void close() {
            Segment.this.removeFromReaders(this);
        }

        @Override
        public String toString() {
            return "SegmentPoller{ uuid='" + uuid + '\'' +
                    ", readPosition=" + readPosition +
                    ", lastReadTs=" + lastReadTs +
                    '}';
        }
    }

}
