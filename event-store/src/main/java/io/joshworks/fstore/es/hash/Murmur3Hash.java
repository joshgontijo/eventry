package io.joshworks.fstore.es.hash;

import java.nio.ByteBuffer;

public class Murmur3Hash implements Hash {

    private static final int SEED = 0x3741b28c;

    @Override
    public int hash32(ByteBuffer data) {
        return Murmur3.hash32(data);
    }

    @Override
    public int hash32(ByteBuffer data, int seed) {
        return Murmur3.hash32(data, seed);
    }

    @Override
    public int hash32(byte[] data) {
        return Murmur3.hash32(data);
    }

    @Override
    public long hash64(byte[] data) {
        return Murmur3.hash64(data);
    }

    @Override
    public int hash32(byte[] data, int seed) {
        return Murmur3.hash32(data, seed);
    }
}
