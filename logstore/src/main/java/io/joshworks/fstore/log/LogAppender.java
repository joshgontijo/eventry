package io.joshworks.fstore.log;

import io.joshworks.fstore.api.Serializer;
import io.joshworks.fstore.utils.io.DiskStorage;
import io.joshworks.fstore.utils.io.RuntimeIOException;
import io.joshworks.fstore.utils.io.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class LogAppender<T> implements Log<T> {

    private static final Logger logger = LoggerFactory.getLogger(LogAppender.class);

    private final int segmentBitShift;

    private final File directory;
    private final Serializer<T> serializer;

    private final List<Log<T>> segments;
    private Log<T> currentSegment;

    private final Metadata metadata;
    private long position;
    private final int segmentSize;


    public static void main(String[] args) {
        long a = 1;
        System.out.println(Long.toBinaryString(a));
        System.out.println(Long.toBinaryString(-a));
    }

    public static <T> LogAppender<T> create(File directory, Serializer<T> serializer, int segmentSize) {
        if (LogFileUtils.metadataExists(directory)) {
            throw new RuntimeException("Metadata file found, use open instead");
        }

        try {
            LogFileUtils.createRoot(directory);

            Metadata metadata = new Metadata().segmentSize(segmentSize);
            LogFileUtils.writeMetadata(directory, metadata);

            return new LogAppender<>(directory, serializer, metadata);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    public static <T> LogAppender<T> open(File directory, Serializer<T> serializer) {
        if (!directory.exists()) {
            throw new IllegalArgumentException("Directory doesn't exist");
        }
        if (!directory.isDirectory()) {
            throw new IllegalArgumentException(directory.getName() + " is not a directory");
        }
        if (LogFileUtils.metadataExists(directory)) {
            throw new IllegalStateException("Directory doesn't contain any metadata information");
        }
        try {
            Metadata metadata = LogFileUtils.readMetadata(directory);

            return new LogAppender<>(directory, serializer, metadata);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    private LogAppender(File directory, Serializer<T> serializer, Metadata metadata) {
        this.directory = directory;
        this.serializer = serializer;
        this.metadata = metadata;

        this.position = metadata.lastPosition();
        this.segmentSize = metadata.segmentSize();
        this.segmentBitShift = metadata.segmentBitShift();

        this.segments = LogFileUtils.loadSegments(directory, f -> openSegment(f, serializer, 0, false));
        if (this.segments.isEmpty()) {
            File segmentFile = LogFileUtils.newSegmentFile(directory, this.segments.size());
            this.currentSegment = createSegment(segmentFile, serializer, segmentSize);
            this.segments.add(currentSegment);
        } else {
            this.currentSegment = segments.get(segments.size() - 1);
        }
    }


    protected Log<T> createSegment(File segmentFile, Serializer<T> serializer, int segmentSize) {
        Storage storage = new DiskStorage(segmentFile, segmentSize);
        return LogSegment.create(storage, serializer);
    }

    protected Log<T> openSegment(File segmentFile, Serializer<T> serializer, long position, boolean checkIntegrity) {
        Storage storage = new DiskStorage(segmentFile, segmentSize);
        return LogSegment.open(storage, serializer, position, checkIntegrity);
    }

    private Log<T> roll() {
        try {
            logger.info("Rolling appender");
            metadata.lastPosition(position);

            LogFileUtils.writeMetadata(directory, metadata);
            currentSegment.flush();

            File newSegmentFile = LogFileUtils.newSegmentFile(directory, this.segments.size());

            Log<T> newSegment = createSegment(newSegmentFile, serializer, metadata.segmentSize());
            this.segments.add(newSegment);
            return newSegment;
        } catch (IOException e) {
            throw new RuntimeIOException("Could not close segment file", e);
        }
    }

    private static int getSegment(long position) {
        int pos = position == 0 ? 1 : (int) (position / SEGMENT_MULTIPLIER);
        return pos - 1; //requires offset to access the array, here values start with 1
    }

    private static long toSegmentedPosition(int segmentIdx, long position) {
        return SEGMENT_MULTIPLIER * segmentIdx + position;
    }

    private static long getPositionOnSegment(long position) {
        return position % SEGMENT_MULTIPLIER;
    }

    @Override
    public long append(T data) {
        long segmentPosition = currentSegment.append(data);
        this.position = toSegmentedPosition(segments.size(), segmentPosition);
        if (segmentPosition > segmentSize) {
            currentSegment = roll();
        }
        return this.position;
    }

    @Override
    public Scanner<T> scanner() {
        return new RollingSegmentReader<>(segments, 0);
    }

    @Override
    public Scanner<T> scanner(long position) {
        return new RollingSegmentReader<>(segments, position);
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public T get(long position) {
        int segmentIdx = getSegment(position);
        if (segmentIdx < 0) {
            return null;
        }
        long positionOnSegment = getPositionOnSegment(position);
        Log<T> segment = segments.get(segmentIdx);
        if (segment != null) {
            return segment.get(positionOnSegment);
        }
        return null;
    }

    @Override
    public T get(long position, int length) {
        int segmentIdx = getSegment(position);
        long positionOnSegment = getPositionOnSegment(position);
        if (segmentIdx < 0) {
            return null;
        }
        Log<T> segment = segments.get(segmentIdx);
        if (segment != null) {
            return segment.get(positionOnSegment, length);
        }
        return null;

    }

    @Override
    public void close() throws IOException {
        metadata.lastPosition(position);
        LogFileUtils.writeMetadata(directory, metadata);
        for (Log<T> segment : segments) {
            segment.close();
        }
    }

    @Override
    public void flush() throws IOException {
        currentSegment.flush();
    }

    private static class RollingSegmentReader<T> implements Scanner<T> {

        private final List<Log<T>> segments;
        private Scanner<T> current;
        private int segmentIdx;

        public RollingSegmentReader(List<Log<T>> segments, long position) {
            this.segments = segments;
            if (!segments.isEmpty()) {
                this.segmentIdx = getSegment(position);
                this.current = segments.get(segmentIdx).scanner(getPositionOnSegment(position));
            }
        }

        @Override
        public long position() {
            return toSegmentedPosition(segmentIdx + 1, current.position());
        }

        @Override
        public Iterator<T> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            if (current == null) {
                return false;
            }
            boolean hasNext = current.hasNext();
            if (!hasNext) {
                if (++segmentIdx >= segments.size()) {
                    return false;
                }
                current = segments.get(segmentIdx).scanner();
                return current.hasNext();
            }
            return true;

        }

        @Override
        public T next() {
            return current.next();
        }
    }

}
