package io.joshworks.fstore.log.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CompressedBlockSerializer<T> implements Serializer<Block<T>> {

    private final Codec codec;
    private final Serializer<T> serializer;

    public CompressedBlockSerializer(Codec codec, Serializer<T> serializer) {
        this.codec = codec;
        this.serializer = serializer;
    }

    @Override
    public ByteBuffer toBytes(Block<T> data) {
        ByteBuffer dest = ByteBuffer.allocate(data.uncompressedSize() + 16);
        writeTo(data, dest);
        return (ByteBuffer) dest.flip();
    }

    @Override
    public void writeTo(Block<T> block, ByteBuffer dest) {
        if (block.readOnly()) {
            throw new IllegalStateException("Block is read only");
        }
        ByteBuffer buffer = block.buffer();
        int entryCount = block.entryCount();
        List<Integer> lengths = block.entriesLength();

        if (buffer.remaining() == 0) {
            throw new IllegalStateException("Block is empty");
        }

        ByteBuffer withLength = ByteBuffer.allocate(block.uncompressedSize());
        withLength.putInt(entryCount);
        for (int i = 0; i < entryCount; i++) {
            withLength.putInt(lengths.get(i));
        }
        withLength.put(buffer);

        withLength.flip();
        ByteBuffer compressed = codec.compress(withLength);
        if (dest.remaining() < compressed.remaining()) {
            throw new IllegalStateException("Not enough space to store block");
        }
        dest.put(compressed);
    }

    @Override
    public Block<T> fromBytes(ByteBuffer data) {
        ByteBuffer decompressed = codec.decompress(data);
        int entryCount = decompressed.getInt();
        List<Integer> lengths = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            lengths.add(decompressed.getInt());
        }

        return new Block<>(serializer, lengths, decompressed.slice().asReadOnlyBuffer());
    }
}
