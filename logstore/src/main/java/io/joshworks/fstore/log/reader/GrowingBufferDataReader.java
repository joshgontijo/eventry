package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Log;

import java.nio.ByteBuffer;

//NOT THREAD SAFE - can be used for each reader though
public class GrowingBufferDataReader extends ChecksumDataReader {

    private ByteBuffer buffer;
    private final boolean direct;

    public GrowingBufferDataReader(Storage storage, int bufferSize, boolean direct, double checksumProb) {
        super(storage, checksumProb);
        this.buffer = direct ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize);
        this.direct = direct;
    }

    @Override
    public ByteBuffer read(long position) {
        buffer.clear();
        storage.read(position, buffer);
        buffer.flip();

        int length = buffer.getInt();
        if(length == Log.EOF || length == 0) {
            return ByteBuffer.allocate(0);
        }

        int checksum = buffer.getInt();

        if (length + Log.HEADER_SIZE > buffer.capacity()) {
            grow(length + Log.HEADER_SIZE);
            return read(position);
        }

        buffer.limit(buffer.position() + length);
        checksum(checksum, buffer);
        return buffer;
    }

    @Override
    public ByteBuffer read(long position, int size) {
        return read(position);
    }

    private void grow(int length) {
        if(length < buffer.capacity()) {
            throw new IllegalStateException("New length must be greater than original capacity");
        }
        buffer = direct ? ByteBuffer.allocateDirect(length) : ByteBuffer.allocate(length);
    }

}
