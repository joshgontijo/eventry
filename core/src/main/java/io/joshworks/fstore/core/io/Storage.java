package io.joshworks.fstore.core.io;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public abstract class Storage implements Flushable, Closeable {

    protected RandomAccessFile raf;
    protected FileChannel channel;
    protected String name;
    protected FileLock lock;
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
            this.channel = raf.getChannel();
            this.lock = this.channel.lock();
        } catch (Exception e) {
            IOUtils.closeQuietly(raf);
            IOUtils.closeQuietly(channel);
            IOUtils.releaseLock(lock);
            throw new RuntimeException("Failed to open storage of " + target.getName(), e);
        }
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
        IOUtils.releaseLock(lock);
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
