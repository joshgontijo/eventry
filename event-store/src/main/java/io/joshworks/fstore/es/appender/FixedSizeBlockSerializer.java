package io.joshworks.fstore.es.appender;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class FixedSizeBlockSerializer<T> implements Serializer<FixedSizeEntryBlock<T>> {

    private final Codec codec;
    private Serializer<T> serializer;
    private final int entrySize;

    public FixedSizeBlockSerializer(Serializer<T> serializer, int entrySize) {
        this.codec = new SnappyCodec();
        this.serializer = serializer;
        this.entrySize = entrySize;
    }

    @Override
    public ByteBuffer toBytes(FixedSizeEntryBlock<T> data) {
        return codec.compress(data.buffer());
    }

    @Override
    public void writeTo(FixedSizeEntryBlock<T> data, ByteBuffer dest) {
        //do nothing
    }

    @Override
    public FixedSizeEntryBlock<T> fromBytes(ByteBuffer buffer) {
        ByteBuffer data = codec.decompress(buffer);
        return new FixedSizeEntryBlock<>(serializer, data, entrySize);
    }
}
