package io.joshworks.fstore.serializer.json;

import com.google.gson.Gson;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.StringSerializer;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;


public class JsonSerializer<T> implements Serializer<T> {

    private static final Gson gson = new Gson();
    private static final StringSerializer stringSerializer = new StringSerializer();

    private final Type type;

    private JsonSerializer(Type type) {
        this.type = type;
    }

    public static <T> JsonSerializer<T> of(Class<T> type) {
        return new JsonSerializer<>(type);
    }

    public static <T> JsonSerializer<T> of(Type type) {
        return new JsonSerializer<>(type);
    }

    @Override
    public ByteBuffer toBytes(T event) {
        return stringSerializer.toBytes(gson.toJson(event));
    }

    @Override
    public void writeTo(T data, ByteBuffer dest) {
        stringSerializer.writeTo(gson.toJson(data), dest);
    }

    @Override
    public T fromBytes(ByteBuffer data) {
        return gson.fromJson(stringSerializer.fromBytes(data), type);
    }
}
