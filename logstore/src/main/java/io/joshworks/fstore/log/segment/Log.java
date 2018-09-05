package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.TimeoutReader;
import io.joshworks.fstore.log.Writer;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.stream.Stream;

public interface Log<T> extends Writer<T>, Closeable {

    int LENGTH_SIZE = Integer.BYTES; //length
    int CHECKSUM_SIZE = Integer.BYTES; //crc32
    int MAIN_HEADER = LENGTH_SIZE + CHECKSUM_SIZE; //header before the entry
    int SECUNDARY_HEADER = LENGTH_SIZE; //header after the entry, used only for backward reads

    int HEADER_OVERHEAD = MAIN_HEADER + LENGTH_SIZE; //length + crc32
    byte[] EOL = ByteBuffer.allocate(HEADER_OVERHEAD).putInt(0).putInt(0).array(); //eof header, -1 length, 0 crc
    long START = Header.BYTES;

    String name();

    Stream<T> stream(Direction direction);

    LogIterator<T> iterator(long position, Direction direction);

    LogIterator<T> iterator(Direction direction);

    long position();

    Marker marker();

    T get(long position);

    PollingSubscriber<T> poller(long position);

    PollingSubscriber<T> poller();

    long size();

    Set<TimeoutReader> readers();

    SegmentState rebuildState(long lastKnownPosition);

    void delete();

    void roll(int level);

    void roll(int level, ByteBuffer footer);

    ByteBuffer readFooter();

    boolean readOnly();

    long entries();

    int level();

    long created();

}
