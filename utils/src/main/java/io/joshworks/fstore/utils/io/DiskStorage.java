package io.joshworks.fstore.utils.io;


import io.joshworks.fstore.utils.IOUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DiskStorage implements Storage {

    private final RandomAccessFile raf;
    private final FileChannel channel;
    //TODO lock ?

    public DiskStorage(RandomAccessFile raf) {
        this.raf = raf;
        this.channel = raf.getChannel();
    }

    @Override
    public int write(long position, ByteBuffer data) {
        try {
            int written = 0;
            while (data.hasRemaining()) {
                written += channel.write(data, position + written);
            }
            return written;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int read(long position, ByteBuffer data) {
        try {
            int read = 0;
            while (data.hasRemaining()) {
                read += channel.read(data, position);
            }
            return read;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long size() {
        try {
            return raf.length();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        IOUtils.closeQuietly(channel);
        IOUtils.closeQuietly(raf);
    }

    @Override
    public void flush() throws IOException {
        if (channel.isOpen())
            channel.force(true);
    }
}
