package io.joshworks.fstore.es.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.block.Block;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

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

    public Queue<T> queueEntries() {
        int entryCount = entryCount();
        Queue<T> entries = new LinkedList<>();
        ByteBuffer readBuffer = readOnlyBuffer();

        for (int i = 0; i < entryCount; i++) {
            T entry = readEntry(readBuffer, serializer, entrySize);
            entries.add(entry);
        }

        return entries;
    }

    @Override
    public List<T> entries() {
        int entryCount = entryCount();
        List<T> entries = new ArrayList<>(entryCount);
        ByteBuffer readBuffer = readOnlyBuffer();

        for (int i = 0; i < entryCount; i++) {
            T entry = readEntry(readBuffer, serializer, entrySize);
            entries.add(entry);
        }

        return entries;
    }

    @Override
    public T get(int idx) {
        if (idx > entryCount()) {
            return null;
        }

        ByteBuffer readOnlyBb = pack();
        readOnlyBb.position(idx * entrySize);
        return readEntry(readOnlyBb, serializer, entrySize);
    }

}
