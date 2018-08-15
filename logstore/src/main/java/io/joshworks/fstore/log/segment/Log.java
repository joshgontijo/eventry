package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.Writer;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

public interface Log<T> extends Writer<T>, Closeable {

    int ENTRY_HEADER_SIZE = Integer.BYTES * 2; //length + crc32
    byte[] EOL = ByteBuffer.allocate(ENTRY_HEADER_SIZE).putInt(0).putInt(0).array(); //eof header, -1 length, 0 crc
    long START = Header.BYTES;

    String name();

    LogIterator<T> iterator();

    Stream<T> stream();

    LogIterator<T> iterator(long position);

    long position();

    T get(long position);

    PollingSubscriber<T> poller(long position);

    PollingSubscriber<T> poller();

    long size();

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
