package io.joshworks.fstore.api;

public interface Codec {

    byte[] compress(byte[] data);
    byte[] decompress(byte[] data);

}
