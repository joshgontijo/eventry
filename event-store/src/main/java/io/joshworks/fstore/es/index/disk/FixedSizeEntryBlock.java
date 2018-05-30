package io.joshworks.fstore.es.index.disk;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.segment.block.Block;

import java.nio.ByteBuffer;
import java.util.List;

class FixedSizeEntryBlock<T> extends Block<T> {

    private final int entrySize;

    FixedSizeEntryBlock(Serializer<T> serializer, int maxEntries, int entrySize) {
        super(serializer, maxEntries * entrySize);
        this.entrySize = entrySize;
    }

    FixedSizeEntryBlock(Serializer<T> serializer, List<Integer> lengths, ByteBuffer data, int entrySize) {
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
}
