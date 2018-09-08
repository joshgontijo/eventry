package io.joshworks.fstore.es.index.disk;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class IndexBlockSerializer implements Serializer<IndexBlock> {

    private final Codec codec;

    public IndexBlockSerializer(Codec codec) {
        this.codec = codec;
    }

    @Override
    public ByteBuffer toBytes(IndexBlock data) {
        ByteBuffer packed = data.pack(codec);
        int entries = data.entryCount();

        var withHeader = ByteBuffer.allocate(packed.remaining() + Integer.BYTES);
        withHeader.putInt(entries);
        withHeader.put(packed);

        return withHeader.flip();
    }

    @Override
    public void writeTo(IndexBlock data, ByteBuffer dest) {

    }

    @Override
    public IndexBlock fromBytes(ByteBuffer buffer) {
        int entries = buffer.getInt();
        ByteBuffer decompressed = codec.decompress(buffer);
        return new IndexBlock(decompressed);
    }
}
