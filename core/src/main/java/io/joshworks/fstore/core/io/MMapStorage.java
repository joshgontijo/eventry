package io.joshworks.fstore.core.io;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


public class MMapStorage extends DiskStorage {

    private MappedByteBuffer mbb;
    private final FileChannel.MapMode mode;

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
        position += written;
        return written;
    }

    @Override
    public int read(long position, ByteBuffer data) {
        checkBoundaries(position);
        int pos = (int) position;

        ByteBuffer readOnly = mbb.asReadOnlyBuffer();
        if (pos > readOnly.capacity()) {
            throw new StorageException("Position " + pos + "is out of limit (" + readOnly.capacity() + ")");
        }

        int limit = Math.min(readOnly.capacity() - pos, data.limit());
        readOnly.limit(pos + limit).position(pos);

        data.put(readOnly);
        return limit;
    }

    @Override
    public void position(long position) {
        checkBoundaries(position);
        mbb.position((int) position);
        super.position = position;
    }

    private void checkBoundaries(long position) {
        if (position > Integer.MAX_VALUE) {
            //TODO remap ?
            throw new UnsupportedOperationException("NOT IMPLEMENTED YET");
        }
    }

    private void ensureCapacity(ByteBuffer data) {
        try {
            if (mbb.remaining() < data.remaining()) {
                //grow enough to fit the new data, this should be avoided as much as possible
                this.mbb = grow(raf.length() + (data.remaining() - mbb.remaining()));
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(MMapStorage.class);

    private MappedByteBuffer grow(long newSize) throws IOException {
        logger.warn("Remapping ################################");
        raf.setLength(newSize);
        int prevPos = this.mbb.position();
        flush();
        unmap();
        MappedByteBuffer newMbb = map(raf);
        newMbb.position(prevPos);
        return newMbb;
    }

    //TODO - HOW TO HANDLE FILES WITH MORE THAN Integer.MAX_VALUE ?
    //TODO - SIZE VALIDATION
    //TODO - CHECK MEMORY CONSTRAINTS WHEN OPENING MULTIPLE SEGMENTS, IT CAN BLOW OFF THE MEMORY CAPACITY
    private MappedByteBuffer map(RandomAccessFile raf) {
        try {
            return map(0, raf.length());
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    private MappedByteBuffer map(long from, long to) {
        try {
            return raf.getChannel().map(mode, from, to);
        } catch (Exception e) {
            close();
            throw new StorageException(e);
        }
    }

    @Override
    public long position() {
        return mbb.position();
    }

    @Override
    public void delete() {
        unmap();
        super.delete();
    }

    @Override
    public void close() {
        flush();
        unmap();
        super.close();
    }

    private void unmap() {
        try {
            if(mbb == null) {
                return;
            }
            Class<?> fcClass = channel.getClass();
            Method unmapMethod = fcClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            unmapMethod.setAccessible(true);
            unmapMethod.invoke(null, mbb);
            mbb = null;
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void shrink() {
        int position = mbb.position();
        unmap();
        super.shrink();
        this.mbb = map(0, position);
        this.mbb.position(position);
    }

    @Override
    public void flush() {
        if (mbb != null)
            mbb.force();
    }
}
