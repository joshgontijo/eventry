package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.log.segment.block.Block;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

//Format:
//streamHash-qtd-version1|pos1-version2|pos2-versionN|posN
public class IndexBlock extends Block<IndexEntry> {

    private final List<IndexEntry> cached = new ArrayList<>();

    public IndexBlock(int maxSize) {
        super(null, maxSize);
    }

    protected IndexBlock(ByteBuffer data) {
        super(null, new ArrayList<>(), data);
        this.cached.addAll(unpack(data));
    }

    @Override
    public boolean add(IndexEntry data) {
        if (readOnly) {
            throw new IllegalStateException("Block is read only");
        }
        cached.add(data);
        return cached.size() * IndexEntry.BYTES >= maxSize;
    }

    @Override
    public ByteBuffer pack(Codec codec) {
        if (cached.isEmpty()) {
            return ByteBuffer.allocate(0);
        }
        int maxVersionSizeOverhead = entryCount() * Integer.BYTES;
        var packed = ByteBuffer.allocate(buffer.position() + maxVersionSizeOverhead);

        IndexEntry last = null;
        List<Integer> versions = new ArrayList<>();
        List<Long> positions = new ArrayList<>();
        for (IndexEntry indexEntry : cached) {
            if (last == null) {
                last = indexEntry;
            }
            if (last.stream != indexEntry.stream) {
                writeToBuffer(packed, last.stream, versions, positions);
                versions = new ArrayList<>();
                positions = new ArrayList<>();
            }

            versions.add(indexEntry.version);
            positions.add(indexEntry.position);
            last = indexEntry;
        }
        if(last != null && !versions.isEmpty()) {
            writeToBuffer(packed, last.stream, versions, positions);
        }

        packed.flip();
        return codec.compress(packed);
    }

    private void writeToBuffer(ByteBuffer buffer, long stream, List<Integer> versions, List<Long> positions) {
        buffer.putLong(stream);
        buffer.putInt(versions.size());
        for (int i = 0; i < versions.size(); i++) {
            buffer.putInt(versions.get(i));
            buffer.putLong(positions.get(i));
        }
    }

    private List<IndexEntry> unpack(ByteBuffer readBuffer) {
        List<IndexEntry> entries = new ArrayList<>();
        while (readBuffer.hasRemaining()) {
            long stream = readBuffer.getLong();
            int numVersions = readBuffer.getInt();
            for (int i = 0; i < numVersions; i++) {
                int version = readBuffer.getInt();
                long position = readBuffer.getLong();
                entries.add(IndexEntry.of(stream, version, position));
            }
        }
        return entries;
    }

    @Override
    public int entryCount() {
        return cached.size();
    }

    @Override
    public List<IndexEntry> entries() {
        return new ArrayList<>(cached);
    }

    @Override
    public IndexEntry first() {
        if (cached.isEmpty()) {
            return null;
        }
        return cached.get(0);
    }

    @Override
    public IndexEntry last() {
        if (cached.isEmpty()) {
            return null;
        }
        return cached.get(cached.size() - 1);
    }

    @Override
    public IndexEntry get(int pos) {
        return cached.get(pos);
    }

    @Override
    public Iterator<IndexEntry> iterator() {
        return entries().iterator();
    }


    @Override
    public boolean isEmpty() {
        return cached.isEmpty();
    }
}
