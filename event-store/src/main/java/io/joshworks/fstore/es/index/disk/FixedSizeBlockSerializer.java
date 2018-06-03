package io.joshworks.fstore.es.index.disk;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.segment.block.FixedSizeEntryBlock;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FixedSizeBlockSerializer<T> implements Serializer<FixedSizeEntryBlock<T>> {

    private final Codec codec;
    private Serializer<T> serializer;
    private final int entrySize;

    FixedSizeBlockSerializer(Serializer<T> serializer, int entrySize) {
        this.codec = new SnappyCodec();
        this.serializer = serializer;
        this.entrySize = entrySize;
    }

    @Override
    public ByteBuffer toBytes(FixedSizeEntryBlock<T> data) {
        return data.pack(codec);
    }

    @Override
    public void writeTo(FixedSizeEntryBlock<T> data, ByteBuffer dest) {
        //do nothing
    }

    @Override
    public FixedSizeEntryBlock<T> fromBytes(ByteBuffer buffer) {
        ByteBuffer data = codec.decompress(buffer);
        List<Integer> lengths = IntStream.range(0, data.limit() / entrySize).boxed().map(i -> entrySize).collect(Collectors.toList());
        return new FixedSizeEntryBlock<>(serializer, lengths, data, entrySize);
    }
}
