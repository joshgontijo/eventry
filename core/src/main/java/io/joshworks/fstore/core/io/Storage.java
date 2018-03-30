package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.RuntimeIOException;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public abstract class Storage implements Flushable, Closeable {

    protected final RandomAccessFile raf;
    protected final FileChannel channel;
    protected final String name;
    protected long size;

    private static final long DEFAULT_LENGTH = 10485760L;//10mb

    public Storage(File target) {
        this(target, DEFAULT_LENGTH);
    }

    public Storage(File target, long length) {
        this.raf = IOUtils.readWriteRandomAccessFile(target);
        try {
            this.name = target.getName();
            this.raf.setLength(length);
        } catch (IOException e) {
            IOUtils.closeQuietly(raf);
            throw RuntimeIOException.of(e);
        }
        this.channel = raf.getChannel();
    }

    public abstract int write(long position, ByteBuffer data);

    public abstract int read(long position, ByteBuffer data);

    protected void ensureNonEmpty(ByteBuffer data) {
        if (data.remaining() == 0) {
            throw new IllegalArgumentException("Cannot store empty record");
        }
    }

    public long size() {
        return size;
    }

    @Override
    public void close() throws IOException {
        flush();
        raf.setLength(size);
        IOUtils.closeQuietly(channel);
        IOUtils.closeQuietly(raf);
    }

    @Override
    public void flush() throws IOException {
        if (channel.isOpen())
            channel.force(true);
    }

    public String name() {
        return name;
    }

}
