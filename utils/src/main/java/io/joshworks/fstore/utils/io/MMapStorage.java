package io.joshworks.fstore.utils.io;


import io.joshworks.fstore.utils.IOUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


public class MMapStorage implements Storage {

    private final RandomAccessFile raf;
    private MappedByteBuffer mbb;
    private final FileChannel.MapMode mode;

    public MMapStorage(RandomAccessFile raf, FileChannel.MapMode mode) {
        this.mode = mode;
        this.raf = raf;
        try {
            this.mbb = raf.getChannel().map(mode, 0, raf.length());
        } catch (Exception e) {
            IOUtils.closeQuietly(raf);
            throw new RuntimeException(e);
        }
    }

    @Override
    public int write(long position, ByteBuffer data) {
        checkBoundaries(position);

        ensureCapacity(data);
        int written = data.remaining();
        mbb.put(data);
        return written;
    }

    @Override
    public int read(long position, ByteBuffer data) {
        checkBoundaries(position);

        mbb.position((int) position);
        ByteBuffer slice = mbb.slice();
        int start = data.position();
        slice.limit(data.remaining());
        data.put(slice.asReadOnlyBuffer());
        return data.position() - start;
    }

    @Override
    public long size() {
        return mbb.capacity();
    }

    private void checkBoundaries(long position) {
        if(position > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("position must not be greater than " + Integer.MAX_VALUE);
        }
    }

    private void ensureCapacity(ByteBuffer data) {
        try {
            if (mbb.remaining() < data.remaining()) {
                raf.setLength(raf.length() + data.remaining()); //TODO better approach to expand, 10% or enough to fit data ?
                mbb = raf.getChannel().map(mode, 0, raf.length());
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()throws IOException {
        flush();
        mbb = null;
        System.gc(); //this is horrible
        IOUtils.closeQuietly(raf);
    }

    @Override
    public void flush() throws IOException {
        if(mbb != null)
            mbb.force();
    }
}
