package io.joshworks.fstore.log;

import io.joshworks.fstore.api.Codec;
import io.joshworks.fstore.utils.io.RuntimeIOException;
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
    public byte[] decompress(byte[] data) {
        try {
            return Snappy.uncompress(data);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }
}
