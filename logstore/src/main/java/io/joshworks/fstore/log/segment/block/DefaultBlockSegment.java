package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.segment.Type;

public class DefaultBlockSegment<T> extends BlockSegment<T, Block<T>> {

    public DefaultBlockSegment(Storage storage, Serializer<T> serializer, DataReader reader, String magic, Type type, int maxBlockSize) {
        super(storage, serializer, new BlockSerializer<>(serializer, Codec.noCompression()), maxBlockSize, reader, magic, type);
    }

    @Override
    protected Block<T> createBlock(Serializer<T> serializer, int blockSize) {
        return new Block<>(serializer, blockSize);
    }
}
