package io.joshworks.fstore.core.util;

import io.joshworks.fstore.core.io.Storage;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class StatsStorage implements Storage {

    private final AtomicLong reads = new AtomicLong();
    private final AtomicLong writes = new AtomicLong();
    private BigDecimal bytesWritten = BigDecimal.ZERO;
    private BigDecimal bytesRead = BigDecimal.ZERO;

    private final Storage delegate;

    public StatsStorage(Storage delegate) {
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
        int write = delegate.write(data);
        bytesWritten = bytesWritten.add(BigDecimal.valueOf(write));
        return write;
    }

    @Override
    public int read(long position, ByteBuffer data) {
        reads.incrementAndGet();
        int read = delegate.read(position, data);
        bytesRead = bytesRead.add(BigDecimal.valueOf(read));
        return read;
    }

    @Override
    public long length() {
        return delegate.length();
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
    public void shrink() {
        delegate.shrink();
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
