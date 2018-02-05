package io.joshworks.fstore.serializer.json;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.joshworks.fstore.api.Serializer;
import io.joshworks.fstore.serializer.StringSerializer;

import java.nio.ByteBuffer;


public class JsonSerializer<T> implements Serializer<T> {

    private static final Gson gson = new Gson();
    private static final StringSerializer stringSerializer = new StringSerializer();

    @Override
    public ByteBuffer toBytes(T event) {
        return stringSerializer.toBytes(gson.toJson(event));
    }

    @Override
    public T fromBytes(ByteBuffer data) {
        return gson.fromJson(stringSerializer.fromBytes(data), new TypeToken<T>() {}.getType());
    }
}
