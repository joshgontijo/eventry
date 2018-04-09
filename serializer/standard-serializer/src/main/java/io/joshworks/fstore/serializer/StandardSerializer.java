package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.arrays.BooleanArraySerializer;
import io.joshworks.fstore.serializer.arrays.ByteArraySerializer;
import io.joshworks.fstore.serializer.arrays.DoubleArraySerializer;
import io.joshworks.fstore.serializer.arrays.FloatArraySerializer;
import io.joshworks.fstore.serializer.arrays.IntegerArraySerializer;
import io.joshworks.fstore.serializer.arrays.LongArraySerializer;
import io.joshworks.fstore.serializer.arrays.ShortArraySerializer;

import java.util.HashMap;
import java.util.Map;

public class StandardSerializer {

    private static final Map<Class<?>, Serializer<?>> serializers = new HashMap<>();

    static {
        serializers.put(Integer.class, new IntegerSerializer());
        serializers.put(Double.class, new DoubleSerializer());
        serializers.put(Long.class, new LongSerializer());
        serializers.put(Float.class, new FloatSerializer());
        serializers.put(Short.class, new ShortSerializer());
        serializers.put(Boolean.class, new BooleanSerializer());
        serializers.put(Character.class, new CharacterSerializer());
        serializers.put(Byte.class, new ByteSerializer());
        serializers.put(String.class, new StringSerializer());

        serializers.put(Integer.TYPE, new IntegerSerializer());
        serializers.put(Double.TYPE, new DoubleSerializer());
        serializers.put(Long.TYPE, new LongSerializer());
        serializers.put(Float.TYPE, new FloatSerializer());
        serializers.put(Short.TYPE, new ShortSerializer());
        serializers.put(Boolean.TYPE, new BooleanSerializer());
        serializers.put(Character.TYPE, new CharacterSerializer());
        serializers.put(Byte.TYPE, new ByteSerializer());

        serializers.put(int[].class, new IntegerArraySerializer());
        serializers.put(double[].class, new DoubleArraySerializer());
        serializers.put(long[].class, new LongArraySerializer());
        serializers.put(float[].class, new FloatArraySerializer());
        serializers.put(short[].class, new ShortArraySerializer());
        serializers.put(boolean[].class, new BooleanArraySerializer());
        serializers.put(byte[].class, new ByteArraySerializer());

    }

    private StandardSerializer() {


    }

    @SuppressWarnings("unchecked")
    public static <T> Serializer<T> of(Class<T> type) {
        if (type == null) {
            throw new IllegalArgumentException("Class type must be provided");
        }
        Serializer<T> serializer = (Serializer<T>) serializers.get(type);
        if (serializer == null) {
            throw new IllegalArgumentException("Could not found serializer for class " + type.getName());
        }
        return serializer;
    }

    public static Serializer<byte[]> noSerializer() {
        return new ByteArraySerializer();
    }

}
