package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.arrays.BooleanArraySerializer;
import io.joshworks.fstore.serializer.arrays.ByteArraySerializer;
import io.joshworks.fstore.serializer.arrays.DoubleArraySerializer;
import io.joshworks.fstore.serializer.arrays.FloatArraySerializer;
import io.joshworks.fstore.serializer.arrays.IntegerArraySerializer;
import io.joshworks.fstore.serializer.arrays.LongArraySerializer;
import io.joshworks.fstore.serializer.arrays.ShortArraySerializer;
import io.joshworks.fstore.serializer.collection.ListSerializer;
import io.joshworks.fstore.serializer.collection.MapSerializer;
import io.joshworks.fstore.serializer.collection.QueueSerializer;
import io.joshworks.fstore.serializer.collection.SetSerializer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public class Serializers {

    private Serializers() {

    }
    //No serializer
    public static final Serializer<ByteBuffer> BYTE_BUFFER = new DirectSerializer();

    public static final Serializer<String> VSTRING = new VStringSerializer();

    public static final Serializer<Integer> INTEGER = new IntegerSerializer();
    public static final Serializer<String> STRING = new StringSerializer();
    public static final Serializer<Long> LONG = new LongSerializer();
    public static final Serializer<Double> DOUBLE = new DoubleSerializer();
    public static final Serializer<Float> FLOAT = new FloatSerializer();
    public static final Serializer<Short> SHORT = new ShortSerializer();
    public static final Serializer<Boolean> BOOLEAN = new BooleanSerializer();
    public static final Serializer<Character> CHAR = new CharacterSerializer();
    public static final Serializer<Byte> BYTE = new ByteSerializer();

    public static final Serializer<byte[]> BYTE_ARRAY = new ByteArraySerializer();
    public static final Serializer<int[]> INTEGER_ARRAY = new IntegerArraySerializer();
    public static final Serializer<long[]> LONG_ARRAY = new LongArraySerializer();
    public static final Serializer<double[]> DOUBLE_ARRAY = new DoubleArraySerializer();
    public static final Serializer<float[]> FLOAT_ARRAY = new FloatArraySerializer();
    public static final Serializer<short[]> SHORT_ARRAY = new ShortArraySerializer();
    public static final Serializer<boolean[]> BOOLEAN_ARRAY = new BooleanArraySerializer();


    public static <K, V> Serializer<Map<K, V>> mapSerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer, Function<K, Integer> sizeOfKey, Function<V, Integer> sizeOfValue) {
        return new MapSerializer<>(keySerializer, valueSerializer, sizeOfKey, sizeOfValue);
    }

    public static <K, V> Serializer<Map<K, V>> mapSerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer, Function<K, Integer> sizeOfKey, Function<V, Integer> sizeOfValue, Supplier<Map<K, V>> instanceSupplier) {
        return new MapSerializer<>(keySerializer, valueSerializer, sizeOfKey, sizeOfValue, instanceSupplier);
    }

    public static <V> Serializer<List<V>> listSerializer(Serializer<V> valueSerializer, Function<V, Integer> sizeOfValue, Supplier<List<V>> instanceSupplier) {
        return new ListSerializer<>(valueSerializer, sizeOfValue, instanceSupplier);
    }

    public static <V> Serializer<Set<V>> setSerializer(Serializer<V> valueSerializer, Function<V, Integer> sizeOfValue, Supplier<Set<V>> instanceSupplier) {
        return new SetSerializer<>(valueSerializer, sizeOfValue, instanceSupplier);
    }

    public static <V> Serializer<Queue<V>> queueSerializer(Serializer<V> valueSerializer, Function<V, Integer> sizeOfValue, Supplier<Queue<V>> instanceSupplier) {
        return new QueueSerializer<>(valueSerializer, sizeOfValue, instanceSupplier);
    }

}
