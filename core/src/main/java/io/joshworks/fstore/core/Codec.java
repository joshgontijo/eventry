package io.joshworks.fstore.core;

public interface Codec {

    byte[] compress(byte[] data);
    byte[] decompress(byte[] data, int length);

}
