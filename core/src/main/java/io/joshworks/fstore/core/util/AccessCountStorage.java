package io.joshworks.fstore.core.util;

import io.joshworks.fstore.core.io.Storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class AccessCountStorage implements Storage {

    private final AtomicLong reads = new AtomicLong();
    private final AtomicLong writes = new AtomicLong();

    private final Storage delegate;

    public AccessCountStorage(Storage delegate) {
        this.delegate = delegate;
    }

    public long reads() {
        return reads.get();
    }

    public long writes() {
        return writes.get();
    }

    @Override
    public int write(ByteBuffer data) {
        writes.incrementAndGet();
        return delegate.write(data);
    }

    @Override
    public int read(long position, ByteBuffer data) {
        reads.incrementAndGet();
        return delegate.read(position, data);
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public void position(long position) {
        delegate.position(position);
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public void delete() {
        delegate.delete();
    }

    @Override
    public String name() {
        return delegate.name();
    }


    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }
}
