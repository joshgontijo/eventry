package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class BlockSerializer<T> implements Serializer<Block<T>> {

    private final Serializer<T> serializer;
    private final Codec codec;

    public BlockSerializer(Serializer<T> serializer, Codec codec) {
        Objects.requireNonNull(serializer, "Serializer must be provided");
        Objects.requireNonNull(codec, "Codec must be provided");
        this.codec = codec;
        this.serializer = serializer;
    }

    @Override
    public ByteBuffer toBytes(Block<T> block) {
        if (block.readOnly()) {
            throw new IllegalStateException("Block is read only");
        }
        ByteBuffer buffer = block.pack(codec);
        int entryCount = block.entryCount();
        List<Integer> lengths = block.entriesLength();

        if (buffer.remaining() == 0) {
            throw new IllegalStateException("Block is empty");
        }

        ByteBuffer withLength = ByteBuffer.allocate(buffer.remaining() + Integer.BYTES + (Integer.BYTES * entryCount));
        withLength.putInt(entryCount);
        for (int i = 0; i < entryCount; i++) {
            withLength.putInt(lengths.get(i));
        }
        withLength.put(buffer);

        return withLength.flip();
    }

    @Override
    public void writeTo(Block<T> block, ByteBuffer dest) {
        //do nothing
    }


    @Override
    public Block<T> fromBytes(ByteBuffer data) {
        int entryCount = data.getInt();
        List<Integer> lengths = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            lengths.add(data.getInt());
        }

        ByteBuffer block = data.slice().asReadOnlyBuffer();
        ByteBuffer decompressed = codec.decompress(block);
        return new Block<>(serializer, lengths, decompressed);

    }

}
