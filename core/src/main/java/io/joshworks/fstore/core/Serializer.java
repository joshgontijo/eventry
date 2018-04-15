package io.joshworks.fstore.core;

import java.nio.ByteBuffer;

public interface Serializer<T> {

    /**
     *
     * @param data The data to be put in the ByteBuffer
     * @return The flipped {@link ByteBuffer}, read to be read from
     */
    ByteBuffer toBytes(T data);

    /**
     * Writes data to this {@link ByteBuffer}
     * @param data The data to be put in the ByteBuffer
     * @param dest The destination buffer
     */
    void writeTo(T data, ByteBuffer dest);

    /**
     *
     * @param buffer The buffer to read the data from, with the position at the beginning of the data to be read from
     * @return The new instance of the type
     */
    T fromBytes(ByteBuffer buffer);

}
