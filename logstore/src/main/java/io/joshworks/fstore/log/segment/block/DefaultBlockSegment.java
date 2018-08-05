package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.segment.Type;

public class DefaultBlockSegment<T> extends BlockSegment<T, Block<T>> {

    private final Serializer<T> serializer;
    private final int maxBlockSize;

    public DefaultBlockSegment(Storage storage, Serializer<T> serializer, DataReader reader, String magic, Type type, int maxBlockSize) {
        super(storage, new BlockSerializer<>(serializer), reader, magic, type);
        this.serializer = serializer;
        this.maxBlockSize = maxBlockSize;
    }

    @Override
    protected Block<T> createBlock() {
        return new Block<>(serializer, maxBlockSize);
    }
}
