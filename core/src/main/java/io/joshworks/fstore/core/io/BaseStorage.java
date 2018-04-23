package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.RuntimeIOException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;

public abstract class BaseStorage implements  Storage {

    protected RandomAccessFile raf;
    protected FileChannel channel;
    protected File file;
    protected FileLock lock;
    protected long position;

    public BaseStorage(File target) {
        this.raf = IOUtils.readWriteRandomAccessFile(target);
        try {
            this.file = target;
            this.channel = raf.getChannel();
            this.lock = this.channel.lock();
        } catch (Exception e) {
            IOUtils.closeQuietly(raf);
            IOUtils.closeQuietly(channel);
            IOUtils.releaseLock(lock);
            throw new RuntimeException("Failed to open storage of " + target.getName(), e);
        }
    }

    public BaseStorage(File target, long length) {
        this.raf = IOUtils.readWriteRandomAccessFile(target);
        try {
            if (target.length() > length) {
                throw new IllegalStateException("File length (" + target.length() + ") is greater than the specified length (" + length + ")");
            }

            this.file = target;
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

    protected void ensureNonEmpty(ByteBuffer data) {
        if (data.remaining() == 0) {
            throw new IllegalArgumentException("Cannot store empty record");
        }
    }

    @Override
    public long size() {
        return position;
    }

    public void position(long position) {
        try {
            channel.position(position);
            this.position = position;
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    //TODO ATOMIC LONG ?
    @Override
    public long position() {
        return position;
    }

    @Override
    public void delete() {
        try {
            IOUtils.closeQuietly(channel);
            IOUtils.closeQuietly(raf);
            Files.delete(file.toPath());
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
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

    @Override
    public String name() {
        return file.getName();
    }

}
