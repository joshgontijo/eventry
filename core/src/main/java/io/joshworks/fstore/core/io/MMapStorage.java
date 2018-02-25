package io.joshworks.fstore.core.io;


import io.joshworks.fstore.core.RuntimeIOException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
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
        this.mbb = map(raf);

    }

    @Override
    public int write(long position, ByteBuffer data) {
        ensureNonEmpty(data);
        checkBoundaries(position);

        ensureCapacity(position, data);
        int written = data.remaining();
        mbb.position((int) position);
        mbb.put(data);
        return written;
    }

    @Override
    public int read(long position, ByteBuffer data) {
        checkBoundaries(position);

        mbb.position((int) position);
        ByteBuffer slice = mbb.slice();
        int prevPos = data.position();
        slice.limit(data.remaining());
        data.put(slice.asReadOnlyBuffer());
        return data.position() - prevPos;
    }

    private void checkBoundaries(long position) {
        if(position > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("position must less than " + Integer.MAX_VALUE);
        }
    }

    private void ensureCapacity(long position, ByteBuffer data) {
        try {
            if (position + data.remaining() > mbb.capacity()) {
                //TODO better approach to expand, 10% or enough to fit data ?
                raf.setLength(raf.length() + data.remaining());
                this.mbb = map(raf);
            }

        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    private MappedByteBuffer map(RandomAccessFile raf) {
        try {
            return raf.getChannel().map(mode, 0, raf.length());
        } catch (IOException e) {
            try {
                close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            throw RuntimeIOException.of(e);
        }
    }

    @Override
    public void close() throws IOException {
        flush();
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
