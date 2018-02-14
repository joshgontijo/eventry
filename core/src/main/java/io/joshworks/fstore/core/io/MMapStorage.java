package io.joshworks.fstore.core.io;


import io.joshworks.fstore.core.RuntimeIOException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


public class MMapStorage extends Storage {

    private MappedByteBuffer mbb;
    private final FileChannel.MapMode mode;

    public MMapStorage(File file, FileChannel.MapMode mode) {
        super(file);
        this.mode = mode;
        try {
            this.mbb = raf.getChannel().map(mode, 0, raf.length());
        } catch (IOException e) {
            IOUtils.closeQuietly(raf);
            throw RuntimeIOException.of(e);
        }
    }

    public MMapStorage(File file, long length, FileChannel.MapMode mode) {
        super(file, length);
        this.mode = mode;
        try {
            this.mbb = raf.getChannel().map(mode, 0, raf.length());
        } catch (IOException e) {
            IOUtils.closeQuietly(raf);
            throw RuntimeIOException.of(e);
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
            throw new IllegalArgumentException("position must less than " + Integer.MAX_VALUE);
        }
    }

    private void ensureCapacity(ByteBuffer data) {
        try {
            if (mbb.remaining() < data.remaining()) {
                //TODO better approach to expand, 10% or enough to fit data ?
                raf.setLength(raf.length() + data.remaining());
                mbb = raf.getChannel().map(mode, 0, raf.length());
            }

        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    @Override
    public void close() throws IOException {
        mbb = null;
        System.gc(); //this is horrible
        super.close();
    }

    @Override
    public void flush() throws IOException {
        if(mbb != null)
            mbb.force();
        super.flush();
    }
}
