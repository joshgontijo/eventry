package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class HeaderSerializer implements Serializer<Header> {

    @Override
    public ByteBuffer toBytes(Header data) {
        ByteBuffer bb = ByteBuffer.allocate(Header.SIZE);
        writeTo(data, bb);
        return bb.position(0); //do not flip, the header will always have the fixed size

    }

    @Override
    public void writeTo(Header data, ByteBuffer dest) {
        byte[] magic = data.magic.getBytes(StandardCharsets.UTF_8);

        dest.put(magic);
        dest.putInt(data.type.val);
        dest.putLong(data.created);
        dest.putInt(data.level);
        dest.putInt(data.entries);
    }

    @Override
    public Header fromBytes(ByteBuffer buffer) {
        if (buffer.remaining() != Header.SIZE) {
            throw new IllegalStateException("Expected " + Header.SIZE + " header length");
        }

        byte[] magicData = new byte[Header.LOG_MAGIC.length()];
        buffer.get(magicData);
        String magic = new String(magicData, StandardCharsets.UTF_8);

        int type = buffer.getInt();
        long created = buffer.getLong();
        int level = buffer.getInt();
        int entries = buffer.getInt();

        if(created == 0 || type == 0) { //empty
            return Header.EMPTY;
        }

        if(!Header.LOG_MAGIC.equals(magic)) {
            throw new CorruptedSegmentException("Invalid segment magic: '" + magic + "'");
        }

        return new Header(entries, created, level, Type.of(type));

    }
}
