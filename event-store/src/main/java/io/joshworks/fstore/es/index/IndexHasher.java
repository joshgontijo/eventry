package io.joshworks.fstore.es.index;

import io.joshworks.fstore.es.hash.Hash;

import java.nio.charset.StandardCharsets;

public class IndexHasher {

    private final Hash highHasher;
    private final Hash lowHasher;

    public IndexHasher(Hash highHasher, Hash lowHasher) {
        this.highHasher = highHasher;
        this.lowHasher = lowHasher;
    }

    public long hash(String stream) {
        byte[] data = stream.getBytes(StandardCharsets.UTF_8);
        return (((long) lowHasher.hash32(data)) << 32) | highHasher.hash32(data);
    }

}
