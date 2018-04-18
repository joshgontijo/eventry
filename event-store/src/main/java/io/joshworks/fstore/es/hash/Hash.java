package io.joshworks.fstore.es.hash;

import java.nio.ByteBuffer;

public interface Hash {

    int hash32(ByteBuffer data);
    int hash32(ByteBuffer data, int seed);

    int hash32(byte[] data);
    int hash32(byte[] data, int seed);

}
