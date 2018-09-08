package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.List;

public class FixedSizeEntryBlock<T> extends Block<T> {

    private final int entrySize;

    public FixedSizeEntryBlock(Serializer<T> serializer, int maxBlockSize, int entrySize) {
        super(serializer, maxBlockSize);
        this.entrySize = entrySize;
    }

    public FixedSizeEntryBlock(Serializer<T> serializer, List<Integer> lengths, ByteBuffer data, int entrySize) {
        super(serializer, lengths, data);
        this.entrySize = entrySize;
    }

    @Override
    public T get(int idx) {
        if (idx > entryCount()) {
            return null;
        }

        ByteBuffer readOnlyBb = readOnlyBuffer();
        readOnlyBb.position(idx * entrySize);
        return readEntry(readOnlyBb, serializer, entrySize);
    }

    public int entrySize() {
        return entrySize;
    }
}
