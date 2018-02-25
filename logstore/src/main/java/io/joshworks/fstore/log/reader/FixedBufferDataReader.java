package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Log;

import java.nio.ByteBuffer;

//NOT THREAD SAFE - can be used for each reader though
public class FixedBufferDataReader extends ChecksumDataReader {

    private final ByteBuffer buffer;
    private static final int DEFAULT_SIZE = 4096;

    public FixedBufferDataReader(Storage storage, boolean direct, double checksumProb) {
        this(storage, DEFAULT_SIZE, direct, checksumProb);
    }

    public FixedBufferDataReader(Storage storage, int bufferSize, boolean direct, double checksumProb) {
        super(storage, checksumProb);
        this.buffer = direct ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize);
    }

    @Override
    public ByteBuffer read(long position) {
        buffer.clear();
        storage.read(position, buffer);
        buffer.flip();

        if(buffer.remaining() == 0) {
            return ByteBuffer.allocate(0);
        }

        int length = buffer.getInt();
        if(length == 0) {
            return ByteBuffer.allocate(0);
        }
        int checksum = buffer.getInt();
        if (length + Log.HEADER_SIZE > buffer.capacity()) {
            return extending(position, length, checksum);
        }

        buffer.limit(buffer.position() + length);
        checksum(checksum, buffer);
        return buffer;
    }

    @Override
    public ByteBuffer read(long position, int size) {
        return read(position);
    }

    private ByteBuffer extending(long position, int length, int checksum) {
        ByteBuffer extra = ByteBuffer.allocate(Log.HEADER_SIZE + length);
        storage.read(position, extra);
        extra.flip();
        extra.position(Log.HEADER_SIZE);
        checksum(checksum, extra);
        return extra;
    }

}
