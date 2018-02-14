package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class CharacterSerializer implements Serializer<Character> {
    @Override
    public ByteBuffer toBytes(Character data) {
        return ByteBuffer.allocate(Character.BYTES).putChar(data).flip();
    }

    @Override
    public Character fromBytes(ByteBuffer data) {
        return data.getChar();
    }
}
