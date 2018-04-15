package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.RuntimeIOException;

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
    protected File file;
    protected FileLock lock;
    protected long size;

    public Storage(File target) {
        this.raf = IOUtils.readWriteRandomAccessFile(target);
        try {
            this.file = target;
//            long length = raf.length();
//            if(length == 0) {
//                throw new IllegalStateException("File length is zero, file expansion can be costly");
//            }
            this.channel = raf.getChannel();
            this.lock = this.channel.lock();
        } catch (Exception e) {
            IOUtils.closeQuietly(raf);
            IOUtils.closeQuietly(channel);
            IOUtils.releaseLock(lock);
            throw new RuntimeException("Failed to open storage of " + target.getName(), e);
        }
    }

    //TODO revisit opening a file when using length (i.e. what if the size was greater than current length)
    public Storage(File target, long length) {
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

    public abstract int write(ByteBuffer data);

    public abstract int read(long position, ByteBuffer data);

    protected void ensureNonEmpty(ByteBuffer data) {
        if (data.remaining() == 0) {
            throw new IllegalArgumentException("Cannot store empty record");
        }
    }

    public long size() {
        return size;
    }

    public void position(long position) {
        try {
            channel.position(position);
            this.size = position;
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    //TODO ATOMIC LONG ?
    public long position() {
        try {
            return channel.position();
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

    public String name() {
        return file.getName();
    }

}
