package io.joshworks.fstore.core.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

//Always read half of the data sent
public class SingleByteMockFileChannel extends FileChannel {

    public ByteArrayOutputStream received = new ByteArrayOutputStream();

    int readOffset = 0;

    @Override
    public int read(ByteBuffer dst) {
        byte[] data = received.toByteArray();
        if(readOffset >= data.length) {
            return -1;
        }
        dst.put(data[readOffset++]);
        return 1;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        throw new UnsupportedEncodingException("NOT IMPLEMENTED");
    }

    @Override
    public int write(ByteBuffer src) {
        received.write(src.get());
        return 1;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        throw new UnsupportedEncodingException("NOT IMPLEMENTED");
    }

    @Override
    public long position() throws IOException {
        throw new UnsupportedEncodingException("NOT IMPLEMENTED");
    }

    @Override
    public FileChannel position(long newPosition) {
        return null;
    }

    @Override
    public long size() throws IOException {
        throw new UnsupportedEncodingException("NOT IMPLEMENTED");
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        throw new UnsupportedEncodingException("NOT IMPLEMENTED");
    }

    @Override
    public void force(boolean metaData) {

    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        throw new UnsupportedEncodingException("NOT IMPLEMENTED");
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        throw new UnsupportedEncodingException("NOT IMPLEMENTED");
    }

    @Override
    public int read(ByteBuffer dst, long position) {
        byte[] data = received.toByteArray();
        dst.put(data, (int)position, (int) (data.length - position));
        return dst.limit();
    }

    @Override
    public int write(ByteBuffer src, long position) {
        return 0;
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) {
        return null;
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) {
        return null;
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) {
        return null;
    }

    @Override
    protected void implCloseChannel() {

    }
}
