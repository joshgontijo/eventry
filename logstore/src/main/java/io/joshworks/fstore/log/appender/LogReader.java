package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.segment.Log;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

//NOT THREAD SAFE
public class LogReader<T> implements LogIterator<T> {

    private final Storage storage;
    private final DataReader reader;
    private final Serializer<T> serializer;

    private T data;
    protected long position;
    private long readAheadPosition;
    private long lastReadSize;

    public LogReader(Storage storage, DataReader reader, Serializer<T> serializer) {
        this(storage, reader, serializer, Log.START);
    }

    public LogReader(Storage storage, DataReader reader, Serializer<T> serializer, long initialPosition) {
        if (initialPosition < Log.START) {
            throw new IllegalArgumentException("Position must be equals or greater than " + Log.START);
        }
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

    @Override
    public void close() {

    }
}
