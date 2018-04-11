package io.joshworks.fstore.core;

import java.nio.ByteBuffer;

public interface Codec {

    ByteBuffer compress(ByteBuffer data);
    ByteBuffer decompress(ByteBuffer compressed);

    static Codec noCompression() {
        return new Codec() {
            @Override
            public ByteBuffer compress(ByteBuffer data) {
                return data;
            }

            @Override
            public ByteBuffer decompress(ByteBuffer compressed) {
                return compressed;
            }
        };
    }

}
