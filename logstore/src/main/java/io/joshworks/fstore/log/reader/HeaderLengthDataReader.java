package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Log;
import io.joshworks.fstore.log.LogSegment;

import java.nio.ByteBuffer;

public class HeaderLengthDataReader extends ChecksumDataReader {

    public HeaderLengthDataReader(Storage storage, double checksumProb) {
        super(storage, checksumProb);

    }

    @Override
    public ByteBuffer read(long position) {
        ByteBuffer header = ByteBuffer.allocate(LogSegment.HEADER_SIZE);
        storage.read(position, header);
        header.flip();

        int length = header.getInt();
        if(length == Log.EOF || length == 0) {
            return ByteBuffer.allocate(0);
        }
        int checksum = header.getInt();

        ByteBuffer data = ByteBuffer.allocate(Log.HEADER_SIZE + length);
        storage.read(position, data);
        data.flip();
        data.position(Log.HEADER_SIZE);
        checksum(checksum, data);

        return data;
    }

    @Override
    public ByteBuffer read(long position, int size) {
        ByteBuffer fullData = ByteBuffer.allocate(LogSegment.HEADER_SIZE + size);
        storage.read(position, fullData);
        fullData.flip();

        int length = fullData.getInt();
        int checksum = fullData.getInt();

        if (length != size) {
            throw new IllegalStateException("Data length doesn't match, expected " + size + " got " + length);
        }

        checksum(checksum, fullData);
        return fullData;
    }

}
