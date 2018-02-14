package io.joshworks.fstore.log;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class Checksum {
    private Checksum(){

    }

    private static final byte[] CRC_SEED = ByteBuffer.allocate(4).putInt(456765723).array();

    public static int checksum(byte[] data) {
        return checksum(data, 0, data.length);
    }

    public static int checksum(ByteBuffer buffer) {
        if (!buffer.hasArray()) {
            throw new IllegalArgumentException("ByteBuffer must be an array backed buffer");
        }
        return checksum(buffer.array(), buffer.position(), buffer.remaining());
    }

    private static int checksum(byte[] data, int offset, int length) {
        final CRC32 checksum = new CRC32();
        checksum.update(CRC_SEED);
        checksum.update(data, offset, length);
        return (int) checksum.getValue();
    }
}
