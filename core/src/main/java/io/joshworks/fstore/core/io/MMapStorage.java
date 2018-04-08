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
            this.mbb = channel.map(mode, 0, raf.length());
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
    public int write(ByteBuffer data) {
        ensureNonEmpty(data);

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
        int prevPos = data.position();
        slice.limit(Math.min(data.remaining(), slice.remaining()));
        data.put(slice.asReadOnlyBuffer());
        return data.position() - prevPos;
    }

    @Override
    public void position(long position) {
        checkBoundaries(position);
        mbb.position((int) position);
    }

    private void checkBoundaries(long position) {
        if(position > Integer.MAX_VALUE) {
            //TODO remap ?
            throw new UnsupportedOperationException("NOT IMPLEMENTED YET");
        }
    }

    private void ensureCapacity(ByteBuffer data) {
        try {
            if (mbb.position() + data.remaining() > mbb.capacity()) {
                //TODO better approach to expand, 10% or enough to fit data ?
                raf.setLength(raf.length() + data.remaining());
                this.mbb = map(raf);
            }

        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    //TODO - HOW TO HANDLE FILES WITH MORE THAN Integer.MAX_VALUE ?
    //TODO - SIZE VALIDATION
    //TODO - CHECK MEMORY CONSTRAINTS WHEN OPENING MULTIPLE SEGMENTS, IT CAN BLOW OFF THE MEMORY CAPACITY
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
    public long position() {
        return mbb.position();
    }

    @Override
    public void close() throws IOException {
        flush();
        mbb = null;
        System.gc(); //this is horrible
        super.close();
    }

    @Override
    public void flush() {
        if(mbb != null)
            mbb.force();
    }
}
