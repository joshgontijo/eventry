package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.LogSegment;

import java.nio.ByteBuffer;

public class HeaderLengthDataReader extends ChecksumDataReader {

    public HeaderLengthDataReader() {
        this(DEFAULT_CHECKUM_PROB);
    }

    public HeaderLengthDataReader(double checksumProb) {
        super(checksumProb);
    }

    @Override
    public ByteBuffer read(Storage storage, long position) {
        ByteBuffer header = ByteBuffer.allocate(LogSegment.ENTRY_HEADER_SIZE);
        storage.read(position, header);
        header.flip();

        if (header.remaining() == 0) {
            return ByteBuffer.allocate(0);
        }

        int length = header.getInt();
        if (length == 0) {
            return ByteBuffer.allocate(0);
        }
        int checksum = header.getInt();

        ByteBuffer data = ByteBuffer.allocate(Log.ENTRY_HEADER_SIZE + length);
        storage.read(position, data);
        data.flip();
        data.position(Log.ENTRY_HEADER_SIZE);
        checksum(checksum, data);

        return data;
    }
}
