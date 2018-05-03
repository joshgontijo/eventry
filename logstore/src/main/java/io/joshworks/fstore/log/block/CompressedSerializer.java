package io.joshworks.fstore.log.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class CompressedSerializer<T> implements Serializer<T> {

    private final Codec codec;
    private final Serializer<T> serializer;


    public CompressedSerializer(Codec codec, Serializer<T> serializer) {
        this.codec = codec;
        this.serializer = serializer;
    }

    @Override
    public ByteBuffer toBytes(T data) {
        ByteBuffer uncompressed = serializer.toBytes(data);
        return codec.compress(uncompressed);
    }

    @Override
    public void writeTo(T data, ByteBuffer dest) {
        ByteBuffer uncompressed = serializer.toBytes(data);
        ByteBuffer compressed = codec.compress(uncompressed);
        dest.put(compressed);
    }

    @Override
    public T fromBytes(ByteBuffer buffer) {
        ByteBuffer uncompressed = codec.decompress(buffer);
        return serializer.fromBytes(uncompressed);
    }
}
