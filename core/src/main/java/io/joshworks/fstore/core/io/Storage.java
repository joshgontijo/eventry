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

    public Storage(File target) {
        this.raf = IOUtils.readWriteRandomAccessFile(target);
        this.channel = raf.getChannel();
    }

    public Storage(File target, long length) {
        this.raf = IOUtils.readWriteRandomAccessFile(target);
        try {
            this.raf.setLength(length);
        } catch (IOException e) {
            IOUtils.closeQuietly(raf);
            throw RuntimeIOException.of(e);
        }
        this.channel = raf.getChannel();
    }

    public abstract int write(long position, ByteBuffer data);

    public abstract int read(long position, ByteBuffer data);

    public abstract long size();

    @Override
    public void close() throws IOException {
        flush();
        IOUtils.closeQuietly(channel);
        IOUtils.closeQuietly(raf);
    }

    @Override
    public void flush() throws IOException {
        if(channel.isOpen())
            channel.force(true);
    }

    //    int write(ByteBuffer data);
//    int read(ByteBuffer data);
}
