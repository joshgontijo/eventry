package io.joshworks.eventry.index;

import io.joshworks.eventry.hash.Hash;

import java.nio.charset.StandardCharsets;

public class StreamHasher {

    private final Hash highHasher;
    private final Hash lowHasher;

    private static final long MASK = (1L << Integer.SIZE) - 1;

    public StreamHasher(Hash highHasher, Hash lowHasher) {
        this.highHasher = highHasher;
        this.lowHasher = lowHasher;
    }

    public long hash(String stream) {
        byte[] data = stream.getBytes(StandardCharsets.UTF_8);
        long high = ((long) highHasher.hash32(data)) << Integer.SIZE;
        int low = lowHasher.hash32(data);

        return high | (MASK & low);
    }

}
