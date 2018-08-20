package io.joshworks.fstore.log.segment;


import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Checksum;
import io.joshworks.fstore.log.LogIterator;
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

    private long entries;
    private final String magic;
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
    public T get(long position) {
        checkBounds(position);
        ByteBuffer data = reader.read(storage, position);
        if (data.remaining() == 0) { //EOF
            return null;
        }
        return serializer.fromBytes(data);
    }

    @Override
    public PollingSubscriber<T> poller(long position) {
        SegmentPoller segmentPoller = new SegmentPoller(storage, reader, serializer, position);
        this.readers.add(segmentPoller);
        return segmentPoller;
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
    public LogIterator<T> iterator() {
        return newLogReader(Log.START);
    }

    @Override
    public Stream<T> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED), false);
    }

    @Override
    public LogIterator<T> iterator(long position) {
        return newLogReader(position);
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
        try {
            logger.info("Restoring log state and checking consistency from position {}", lastKnownPosition);
            LogIterator<T> logIterator = iterator(lastKnownPosition);
            while (logIterator.hasNext()) {
                logIterator.next();
                foundEntries++;
                position = logIterator.position();
            }
        } catch (Exception e) {
            logger.warn("Found inconsistent entry on position {}, segment '{}': {}", position, name(), e.getMessage());
        }
        logger.info("Log state restored, current position {}", position);
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
    private LogReader newLogReader(long pos) {

        while (readers.size() >= 10) {
            try {
                Thread.sleep(1000);
                logger.info("Waiting to acquire reader");
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }

        LogReader logReader = new LogReader(storage, reader, serializer, pos);
        readers.add(logReader);
        return logReader;
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
        ByteBuffer bb = ByteBuffer.allocate(ENTRY_HEADER_SIZE + bytes.remaining());
        bb.putInt(bytes.remaining());
        bb.putInt(Checksum.crc32(bytes));
        bb.put(bytes);

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
        private long lastReadSize;

        LogReader(Storage storage, DataReader reader, Serializer<T> serializer, long initialPosition) {
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
            position += lastReadSize;
            data = readAhead();
            return current;
        }

        private T readAhead() {
            ByteBuffer bb = reader.read(storage, readAheadPosition);
            if (bb.remaining() == 0) { //EOF
                close();
                return null;
            }
            lastReadSize = bb.limit();
            readAheadPosition += bb.limit();
            return serializer.fromBytes(bb);
        }

        @Override
        public void close() {
            readers.remove(this);
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

        private T read() {
            ByteBuffer bb = reader.read(storage, readPosition);
            if (bb.remaining() == 0) { //EOF
                close();
                return null;
            }
            readPosition += bb.limit();
            return serializer.fromBytes(bb);
        }

        private synchronized T tryTake(long sleepInterval, TimeUnit timeUnit) throws InterruptedException {
            if (hasDataAvailable()) {
                this.lastReadTs = System.currentTimeMillis();
                return read();
            }
            waitForData(sleepInterval, timeUnit);
            this.lastReadTs = System.currentTimeMillis();
            return read();
        }

        private boolean hasDataAvailable() {
            return readPosition < Segment.this.position();
        }

        private synchronized T tryPool(long time, TimeUnit timeUnit) throws InterruptedException {
            if (hasDataAvailable()) {
                this.lastReadTs = System.currentTimeMillis();
                return read();
            }
            if (time > 0) {
                waitFor(time, timeUnit);
            }
            this.lastReadTs = System.currentTimeMillis();
            return read();
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
        public T poll() throws InterruptedException {
            return tryPool(NO_SLEEP, TimeUnit.MILLISECONDS);
        }

        @Override
        public T poll(long time, TimeUnit timeUnit) throws InterruptedException {
            return tryPool(time, timeUnit);
        }

        @Override
        public T take() throws InterruptedException {
            return tryTake(VERIFICATION_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        }

        @Override
        public boolean endOfLog() {
            return readOnly() && header.logEnd > 0 && readPosition >= header.logEnd;
        }

        @Override
        public long position() {
            return readPosition;
        }

        @Override
        public void close() {
            readers.remove(this);
        }
    }

}
