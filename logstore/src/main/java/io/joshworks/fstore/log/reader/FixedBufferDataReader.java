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

    public FixedBufferDataReader(int maxRecordSize) {
        this(maxRecordSize, false);
    }

    public FixedBufferDataReader(int maxRecordSize, boolean direct) {
        this(maxRecordSize, direct, DEFAULT_CHECKUM_PROB, DEFAULT_SIZE);
    }

    public FixedBufferDataReader(int maxRecordSize, boolean direct, double checksumProb) {
        this(maxRecordSize, direct, checksumProb, DEFAULT_SIZE);
    }

    public FixedBufferDataReader(int maxRecordSize, boolean direct, double checksumProb, int bufferSize) {
        super(maxRecordSize, checksumProb);
        this.direct = direct;
        this.bufferSize = bufferSize;
    }

    @Override
    public ByteBuffer readForward(Storage storage, long position) {
        ByteBuffer buffer = getBuffer();
        storage.read(position, buffer);
        buffer.flip();

        if (buffer.remaining() == 0) {
            return EMPTY;
        }

        int length = buffer.getInt();
//        if(position == 143775612) {
//            System.err.println(position);
//        }
        checkRecordLength(length, position);
        if (length == 0) {
            return EMPTY;
        }
        if (length + Log.MAIN_HEADER > buffer.capacity()) {
            return extending(storage, position, length);
        }

        int checksum = buffer.getInt();
        buffer.limit(buffer.position() + length);
        checksum(checksum, buffer, position);
        return buffer;
    }

    @Override
    public ByteBuffer readBackward(Storage storage, long position) {

        ByteBuffer buffer = getBuffer();
        int limit = buffer.limit();
        if (position - limit < Log.START) {
            int available = (int) (position - Log.START);
            if (available == 0) {
                return EMPTY;
            }
            buffer.limit(available);
            limit = available;
        }

        storage.read(position - limit, buffer);
        buffer.flip();
        int originalSize = buffer.remaining();
        if (buffer.remaining() == 0) {
            return EMPTY;
        }

        buffer.position(buffer.limit() - Log.LENGTH_SIZE);
        buffer.mark();
        int length = buffer.getInt();
        checkRecordLength(length, position);
        if (length == 0) {
            return EMPTY;
        }

        if (length + Log.HEADER_OVERHEAD > buffer.capacity()) {
            return extending(storage, position, length);
        }

        buffer.reset();
        buffer.limit(buffer.position());
        buffer.position(buffer.position() - length - Log.CHECKSUM_SIZE);
        int checksum = buffer.getInt();
        checksum(checksum, buffer, position);
        return buffer;
    }

    @Override
    public ByteBuffer getBuffer() {
        return direct ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize);
    }

    private void checkRecordLength(int length, long position) {
        if (length > maxRecordSize) {
            throw new IllegalStateException("Record at position " + position + " of size " + length + " must be less than MAX_RECORD_SIZE: " + maxRecordSize);
        }
    }

    private ByteBuffer extending(Storage storage, long position, int length) {
        ByteBuffer extra = ByteBuffer.allocate(Log.MAIN_HEADER + length);
        storage.read(position, extra);
        extra.flip();
        int foundLength = extra.getInt();
        if (foundLength != length) {
            throw new IllegalStateException("Expected length " + length + " got " + foundLength);
        }
        int checksum = extra.getInt();
        checksum(checksum, extra, position);
        return extra;
    }

}
