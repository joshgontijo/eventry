package io.joshworks.fstore.es.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.block.Block;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class FixedSizeEntryBlock<T> extends Block<T> {

    private final int entrySize;

    FixedSizeEntryBlock(Serializer<T> serializer, int maxEntries, int entrySize) {
        super(serializer, maxEntries * entrySize);
        this.entrySize = entrySize;
    }

    FixedSizeEntryBlock(Serializer<T> serializer, ByteBuffer data, int entrySize) {
        this(serializer, -1, entrySize);
        this.data = data;
        this.readOnly = true;
    }

    @Override
    protected ByteBuffer serialize(T data) {
        ByteBuffer serialized = super.serialize(data);
        if (serialized.limit() != entrySize) {
            throw new IllegalArgumentException("Entry must have " + entrySize + " bytes");
        }
        return serialized;
    }

    public int entrySize() {
        return entrySize;
    }

    @Override
    public ByteBuffer buffer() {
        return (ByteBuffer) data.asReadOnlyBuffer().position(0);
    }

    @Override
    public int entryCount() {
        return data.position() / entrySize;
    }

    @Override
    public List<T> entries() {
        int entryCount = entryCount();
        List<T> entries = new ArrayList<>(entryCount);
        ByteBuffer readBuffer = data.asReadOnlyBuffer();
        readBuffer.position(0);

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

        ByteBuffer readOnlyBb = data.asReadOnlyBuffer();
        readOnlyBb.position(idx * entrySize);
        return readEntry(readOnlyBb, serializer, entrySize);
    }

    @Override
    protected int currentHeaderOverhead() {
        return 0;
    }

    @Override
    protected List<Integer> entriesLength() {
        return IntStream.range(0, entryCount())
                .boxed()
                .map(i -> this.entrySize)
                .collect(Collectors.toList());
    }
}
