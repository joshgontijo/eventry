package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FixedSizeBlockSerializer<T> implements Serializer<FixedSizeEntryBlock<T>> {

    private final Codec codec;
    private final Serializer<T> serializer;
    private final int entrySize;

    public FixedSizeBlockSerializer(Serializer<T> serializer, int entrySize, boolean compress) {
        this.codec = compress ? new SnappyCodec() : Codec.noCompression();
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

    private static final Map<Integer, List<Integer>> cachedLengths = new ConcurrentHashMap<>();

    @Override
    public FixedSizeEntryBlock<T> fromBytes(ByteBuffer buffer) {
        ByteBuffer data = codec.decompress(buffer);

        int entries = data.limit() / entrySize;
        //FIXME concurrent modification exception
        List<Integer> lengths = cachedLengths.computeIfAbsent(entries, k -> IntStream.range(0, data.limit() / entrySize).boxed().map(i -> entrySize).collect(Collectors.toList()));

        return new FixedSizeEntryBlock<>(serializer, lengths, data, entrySize);
    }

}
