package io.joshworks.fstore.es.hash;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.nio.ByteBuffer;

public class XXHash implements Hash{

    private final XXHashFactory factory = XXHashFactory.fastestInstance();
    private final XXHash32 hash32 = factory.hash32();
    private static final int SEED = 0x9747b28c;

    @Override
    public int hash32(ByteBuffer data) {
        return hash32(data, SEED);
    }

    @Override
    public int hash32(ByteBuffer data, int seed) {
        return hash32.hash(data.asReadOnlyBuffer(), seed);
    }

    @Override
    public int hash32(byte[] data) {
        return hash32(data, SEED);
    }

    @Override
    public int hash32(byte[] data, int seed) {
        return hash32.hash(data, 0, data.length, seed);
    }
}
