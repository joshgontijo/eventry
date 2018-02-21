package io.joshworks.fstore.core;

import java.nio.ByteBuffer;

public interface Codec {

    ByteBuffer compress(ByteBuffer data);
    ByteBuffer decompress(ByteBuffer compressed);

}
