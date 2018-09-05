package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.Order;
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
    int ENTRY_HEADER_SIZE = LENGTH_SIZE + CHECKSUM_SIZE + LENGTH_SIZE; //length + crc32
    byte[] EOL = ByteBuffer.allocate(ENTRY_HEADER_SIZE).putInt(0).putInt(0).array(); //eof header, -1 length, 0 crc
    long START = Header.BYTES;

    String name();

    LogIterator<T> iterator();

    Stream<T> stream();

    LogIterator<T> iterator(long position);
    LogIterator<T> iterator(Order order);
    LogIterator<T> iterator(long position, Order order);

    long position();

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
