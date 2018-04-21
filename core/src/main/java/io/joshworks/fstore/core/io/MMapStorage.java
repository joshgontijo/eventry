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
        this.mbb = map(raf);
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
        size += written;
        return written;
    }

    @Override
    public int read(long position, ByteBuffer data) {
        checkBoundaries(position);

        ByteBuffer readOnly = mbb.asReadOnlyBuffer();
        int entrySize = data.remaining();

        readOnly.limit((int) position + entrySize).position((int) position);
        if(entrySize > readOnly.remaining()) {
            throw new IllegalStateException("Not available data. Expected " + entrySize + ", got " + readOnly.remaining());
        }

        data.put(readOnly);
        return entrySize;
    }

    @Override
    public void position(long position) {
        checkBoundaries(position);
        mbb.position((int) position);
        super.size = position;
    }

    private void checkBoundaries(long position) {
        if (position > Integer.MAX_VALUE) {
            //TODO remap ?
            throw new UnsupportedOperationException("NOT IMPLEMENTED YET");
        }
    }

    private void ensureCapacity(ByteBuffer data) {
        try {
            if (mbb.position() + data.remaining() > mbb.capacity()) {
                //TODO better approach to expand, 10%, 100% or enough to fit data ?
                this.mbb = grow(raf.length() + data.remaining());
            }

        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    private MappedByteBuffer grow(long newSize) throws IOException {
        raf.setLength(newSize);
        int prevPos = this.mbb.position();
        this.mbb.force();
        MappedByteBuffer newMbb = map(raf);
        newMbb.position(prevPos);
        return newMbb;
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
        if (mbb != null)
            mbb.force();
    }
}
