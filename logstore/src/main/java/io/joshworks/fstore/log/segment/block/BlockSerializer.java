package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BlockSerializer<T> implements Serializer<Block<T>> {

    private final Serializer<T> serializer;

    public BlockSerializer(Serializer<T> serializer) {
        this.serializer = serializer;
    }

    @Override
    public ByteBuffer toBytes(Block<T> data) {
        ByteBuffer dest = ByteBuffer.allocate(data.size() + 16);
        writeTo(data, dest);
        return (ByteBuffer) dest.flip();
    }

    @Override
    public void writeTo(Block<T> block, ByteBuffer dest) {
        ByteBuffer buffer = createBuffer(block);
        dest.put(buffer);
    }


    protected ByteBuffer createBuffer(Block<T> block) {
        if (block.readOnly()) {
            throw new IllegalStateException("Block is read only");
        }
        ByteBuffer buffer = block.pack();
        int entryCount = block.entryCount();
        List<Integer> lengths = block.entriesLength();

        if (buffer.remaining() == 0) {
            throw new IllegalStateException("Block is empty");
        }

        ByteBuffer withLength = ByteBuffer.allocate(block.size());
        withLength.putInt(entryCount);
        for (int i = 0; i < entryCount; i++) {
            withLength.putInt(lengths.get(i));
        }
        withLength.put(buffer);

        withLength.flip();
        return withLength;
    }

    @Override
    public Block<T> fromBytes(ByteBuffer data) {
        int entryCount = data.getInt();
        List<Integer> lengths = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            lengths.add(data.getInt());
        }
        return new Block<>(serializer, lengths, data.slice().asReadOnlyBuffer());

    }

}
