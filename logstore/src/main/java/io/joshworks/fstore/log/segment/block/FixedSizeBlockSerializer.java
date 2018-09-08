package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FixedSizeBlockSerializer<T> implements Serializer<FixedSizeEntryBlock<T>> {

    private final Codec codec;
    private final Serializer<T> serializer;

    public FixedSizeBlockSerializer(Serializer<T> serializer, Codec codec) {
        Objects.requireNonNull(serializer, "Serializer must be provided");
        Objects.requireNonNull(codec, "Codec must be provided");
        this.codec = codec;
        this.serializer = serializer;
    }

    @Override
    public ByteBuffer toBytes(FixedSizeEntryBlock<T> data) {
        ByteBuffer packed = data.pack(codec);
        int entrySize = data.entrySize();
        int entries = data.entryCount();

        var withHeader = ByteBuffer.allocate(packed.remaining() + (Integer.BYTES * 2));
        withHeader.putInt(entrySize);
        withHeader.putInt(entries);
        withHeader.put(packed);

        return withHeader.flip();
    }

    @Override
    public void writeTo(FixedSizeEntryBlock<T> data, ByteBuffer dest) {
        //do nothing
    }

    @Override
    public FixedSizeEntryBlock<T> fromBytes(ByteBuffer buffer) {
        int entrySize = buffer.getInt();
        int entries = buffer.getInt();

        ByteBuffer data = codec.decompress(buffer);
        List<Integer> lengths = IntStream.range(0, entries).map(operand -> entrySize).boxed().collect(Collectors.toList());
        return new FixedSizeEntryBlock<>(serializer, lengths, data, entrySize);
    }

}
