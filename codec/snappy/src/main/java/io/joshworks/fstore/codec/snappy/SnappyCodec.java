package io.joshworks.fstore.codec.snappy;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.RuntimeIOException;
import org.xerial.snappy.Snappy;

import java.io.IOException;

public class SnappyCodec implements Codec {

    @Override
    public byte[] compress(byte[] data) {
        try {
            return Snappy.compress(data);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }


    @Override
    public byte[] decompress(byte[] data, int length) {
        try {
            return Snappy.uncompress(data);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }
}
