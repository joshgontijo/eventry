package io.joshworks.fstore.codec.snappy;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.RuntimeIOException;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SnappyCodec implements Codec {

    @Override
    public ByteBuffer compress(ByteBuffer data) {
        try {

            byte[] compressed = Snappy.compress(getBytes(data));
            return ByteBuffer.wrap(compressed);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }


    @Override
    public ByteBuffer decompress(ByteBuffer compressed) {
        try {
            byte[] uncompressed = Snappy.uncompress(getBytes(compressed));
            return ByteBuffer.wrap(uncompressed);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    private static byte[] getBytes(ByteBuffer data) {
        byte[] b = new byte[data.remaining()];
        data.mark();
        data.get(b);
        data.reset();
        return b;
    }
}
