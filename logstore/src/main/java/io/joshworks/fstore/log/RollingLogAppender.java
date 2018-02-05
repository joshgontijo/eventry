package io.joshworks.fstore.log;

import io.joshworks.fstore.api.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RollingLogAppender<T> implements Log<T> {

    private static final Logger logger = LoggerFactory.getLogger(RollingLogAppender.class);

    static final long SEGMENT_MULTIPLIER = 100000000000L;
    private static final String METADATA_LAST_POSITION = "lastPosition";
    private static final String METADATA_SEGMENT_SIZE = "segmentSize";

    private final File directory;
    private final Serializer<T> serializer;

    private final List<Log<T>> segments;
    private Log<T> currentSegment;

    private final Map<String, Object> metadata;
    private long position;
    private final int segmentSize;


    public static <T> RollingLogAppender<T> create(File directory, Serializer<T> serializer, int segmentSize) {
        if (LogFileUtils.metadataExists(directory)) {
            throw new RuntimeException("Metadata file found, use open instead");
        }

        Map<String, Object> metadata = new HashMap<>();
        try {
            metadata.put(METADATA_LAST_POSITION, 0L);
            metadata.put(METADATA_SEGMENT_SIZE, segmentSize);

            LogFileUtils.createRoot(directory);
            LogFileUtils.writeMetadata(directory, metadata);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new RollingLogAppender<>(directory, serializer, metadata, segmentSize);
    }

    public static <T> RollingLogAppender<T> open(File directory, Serializer<T> serializer) {
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
            Map<String, Object> metadata = LogFileUtils.readMetadata(directory);
            int segmentSize = (int) metadata.get(METADATA_SEGMENT_SIZE);

            return new RollingLogAppender<>(directory, serializer, metadata, segmentSize);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private RollingLogAppender(File directory, Serializer<T> serializer, Map<String, Object> metadata, int segmentSize) {
        this.directory = directory;
        this.serializer = serializer;
        this.metadata = metadata;

        this.position = (long) metadata.getOrDefault(METADATA_LAST_POSITION, 0L);
        this.segmentSize = segmentSize;
        this.segments = LogFileUtils.loadSegments(directory, f -> openSegment(f, serializer, 0, false));
        if (this.segments.isEmpty()) {
            File segmentFile = LogFileUtils.newSegmentFile(directory, this.segments.size());
            this.currentSegment = createSegment(segmentFile, serializer, segmentSize);
            this.segments.add(currentSegment);
        } else {
            this.currentSegment = segments.get(segments.size() - 1);
        }
    }


    protected Log<T> createSegment(File file, Serializer<T> serializer, int size) {
        return LogSegment.create(file, serializer, size);
    }

    protected Log<T> openSegment(File file, Serializer<T> serializer, long position, boolean checkConsistency) {
        return LogSegment.open(file, serializer, position, checkConsistency);
    }


    private Log<T> roll() {
        try {
            logger.info("Rolling appender");
            metadata.put(METADATA_LAST_POSITION, position);
            LogFileUtils.writeMetadata(directory, metadata);
            currentSegment.flush();

            int segmentSize = (int) metadata.get(METADATA_SEGMENT_SIZE);
            File newSegmentFile = LogFileUtils.newSegmentFile(directory, this.segments.size());

            Log<T> newSegment = createSegment(newSegmentFile, serializer, segmentSize);
            this.segments.add(newSegment);
            return newSegment;
        } catch (IOException e) {
            throw new RuntimeException("Could not close segment file", e);
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
    public Reader<T> reader() {
        return new RollingSegmentReader<>(segments, 0);
    }

    @Override
    public Reader<T> reader(long position) {
        return new RollingSegmentReader<>(segments, position);
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public void close() throws IOException {
        for (Log<T> segment : segments) {
            segment.close();
        }
    }

    @Override
    public void flush() throws IOException {
        currentSegment.flush();
    }

    private static class RollingSegmentReader<T> implements Reader<T> {

        private final List<Log<T>> segments;
        private Reader<T> current;
        private int segmentIdx;

        public RollingSegmentReader(List<Log<T>> segments, long position) {
            this.segments = segments;
            if (!segments.isEmpty()) {
                this.segmentIdx = getSegment(position);
                this.current = segments.get(segmentIdx).reader(getPositionOnSegment(position));
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
                current = segments.get(segmentIdx).reader();
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
