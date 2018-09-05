package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.segment.Log;

import java.nio.ByteBuffer;

//THREAD SAFE
public class FixedBufferDataReader extends ChecksumDataReader {

    private static final int DEFAULT_SIZE = 4096;
    public static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

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
    public ByteBuffer readForward(Storage storage, long position) {
        ByteBuffer buffer = getBuffer();
        storage.read(position, buffer);
        buffer.flip();

        if(buffer.remaining() == 0) {
            return EMPTY;
        }

        int length = buffer.getInt();
        if(length == 0) {
            return EMPTY;
        }
        int checksum = buffer.getInt();
        if (length + Log.ENTRY_HEADER_SIZE > buffer.capacity()) {
            return extending(storage, position, length, checksum);
        }

        buffer.limit(buffer.position() + length);
        checksum(checksum, buffer);
        return buffer;
    }

    @Override
    public ByteBuffer readBackward(Storage storage, long position) {

        ByteBuffer buffer = getBuffer();
        int limit = buffer.limit();
        if(position - limit < Log.START) {
            int available = (int) (position - Log.START);
            if(available == 0) {
                return EMPTY;
            }
            buffer.limit(available);
            limit = available;
        }

        storage.read(position - limit, buffer);
        buffer.flip();
        int originalSize = buffer.remaining();

        if(buffer.remaining() == 0) {
            return EMPTY;
        }

        buffer.position(buffer.limit() - Log.LENGTH_SIZE);
        buffer.mark();
        int length = buffer.getInt();
        if(length == 0) {
            return EMPTY;
        }

        buffer.reset();
        buffer.limit(buffer.position());
        buffer.position(buffer.position() - length - Log.CHECKSUM_SIZE);
        int checksum = buffer.getInt();
        if (length + Log.ENTRY_HEADER_SIZE > buffer.capacity()) {
            return extending(storage, position, length, checksum);
        }

        checksum(checksum, buffer);
        return buffer;
    }

    @Override
    public ByteBuffer getBuffer() {
        return direct ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize);
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
