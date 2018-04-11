package io.joshworks.lsmtree.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Block<T> {

    private final Serializer<T> serializer;
    private ByteBuffer buffer;

    private boolean readOnly;
    private int entryCount;
    private int[] lengths = new int[64];

    public Block(Serializer<T> serializer, int maxSize) {
        this.serializer = serializer;
        this.buffer = ByteBuffer.allocate(maxSize);
    }

    private Block(Serializer<T> serializer, int entryCount, int[] lengths, ByteBuffer data) {
        this.serializer = serializer;
        this.entryCount = entryCount;
        this.lengths = lengths;
        this.buffer = data;
        this.readOnly = true;
    }

    public boolean add(T data) {
        if(readOnly) {
            throw new IllegalStateException("Block is read only");
        }
        ByteBuffer bb = serializer.toBytes(data);
        addLength(bb.limit());

        if (bb.limit() > buffer.capacity() || bb.limit() > buffer.remaining()) {
            ByteBuffer tempBb = ByteBuffer.allocate(buffer.capacity() + bb.limit());
            buffer.flip();
            tempBb.put(buffer).put(bb);
            buffer = tempBb;
            return true;
        }

        buffer.put(bb);
        return buffer.remaining() == 0;
    }

    private void addLength(int offset) {
        if (entryCount >= lengths.length) {
            int[] resized = newArray(lengths.length * 2);
            System.arraycopy(lengths, 0, resized, 0, lengths.length);
        }
        lengths[entryCount++] = offset;
    }

    private int[] newArray(int size) {
        int[] array = new int[size];
        Arrays.fill(array, -1);
        return array;
    }

    public long size() {
        return buffer.limit();
    }

    public int entries() {
        return entryCount;
    }

    public T first() {
        if (lengths[0] < 0) {
            return null;
        }
        ByteBuffer bb = ByteBuffer.allocate(lengths[0]);

        int pos = buffer.position();

        buffer.position(0).limit(lengths[0]);
        bb.put(buffer);
        buffer.limit(buffer.capacity()).position(pos);

        bb.flip();
        return serializer.fromBytes(bb);
    }

    public ByteBuffer compress(Codec compressor) {
        if(readOnly) {
            throw new IllegalStateException("Block is read only");
        }
        buffer.flip();
        if (buffer.remaining() == 0) {
            return ByteBuffer.allocate(0);
        }
        if(entryCount != lengths.length) {
            throw new IllegalStateException("entryCount doesn't match length count");
        }

        ByteBuffer withLength = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES * entryCount + buffer.limit());
        withLength.putInt(entryCount);
        for (int i = 0; i < entryCount; i++) {
            withLength.putInt(lengths[i]);
        }
        withLength.put(buffer);

        withLength.flip();
        readOnly = true;
        return compressor.compress(withLength);
    }

    public static <T> Block<T> load(Codec compressor, Serializer<T> serializer,  ByteBuffer data) {
        int entryCount = data.getInt();
        int[] lengths = new int[entryCount];
        for (int i = 0; i < entryCount; i++) {
            lengths[i] = data.getInt();
        }
        ByteBuffer decompressed = compressor.decompress(data);

        return new Block<>(serializer, entryCount, lengths, decompressed);
    }

    private static class CompressedBlockSerializer<T> implements Serializer<Block<T>> {

        private final Codec codec;

        private CompressedBlockSerializer(Codec codec) {
            this.codec = codec;
        }

        @Override
        public ByteBuffer toBytes(Block<T> data) {
            return null;
        }

        @Override
        public Block<T> fromBytes(ByteBuffer buffer) {
            return null;
        }
    }

}
