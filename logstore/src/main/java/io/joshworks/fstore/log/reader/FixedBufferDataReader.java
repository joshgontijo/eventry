package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.segment.Log;

import java.nio.ByteBuffer;

//NOT THREAD SAFE - can be used for each reader though
public class FixedBufferDataReader extends ChecksumDataReader {

    private static final int DEFAULT_SIZE = 4096;

    private final boolean direct;
    private final int bufferSize;

    public FixedBufferDataReader() {
        this(false);
    }

    public FixedBufferDataReader(boolean direct) {
        this(direct, DEFAULT_CHECKUM_PROB, DEFAULT_SIZE);
    }

    public FixedBufferDataReader(boolean direct, double checksumProb) {
        this(direct, checksumProb, DEFAULT_SIZE);
    }

    public FixedBufferDataReader(boolean direct, double checksumProb, int bufferSize) {
        super(checksumProb);
        this.direct = direct;
        this.bufferSize = bufferSize;
    }

    @Override
    public ByteBuffer read(Storage storage, long position) {
        ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize);
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
        if (length + Log.ENTRY_HEADER_SIZE > buffer.capacity()) {
            return extending(storage, position, length, checksum);
        }

        buffer.limit(buffer.position() + length);
        checksum(checksum, buffer);
        return buffer;
    }

    private ByteBuffer extending(Storage storage, long position, int length, int checksum) {
        ByteBuffer extra = ByteBuffer.allocate(Log.ENTRY_HEADER_SIZE + length);
        storage.read(position, extra);
        extra.flip();
        extra.position(Log.ENTRY_HEADER_SIZE);
        checksum(checksum, extra);
        return extra;
    }

}
