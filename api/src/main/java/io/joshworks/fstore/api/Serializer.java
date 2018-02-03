package io.joshworks.fstore.api;

public interface Serializer<T> {
    byte[] toBytes(T data);

    T fromBytes(byte[] data);

    T fromBytes(byte[] data, int offset, int length);
}
