package io.joshworks.fstore.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Java9ImmutableMapSerializer extends Serializer<Map<Object, Object>> {

    private static final boolean DOES_NOT_ACCEPT_NULL = true;
    private static final boolean IMMUTABLE = true;

    public Java9ImmutableMapSerializer() {
        super(DOES_NOT_ACCEPT_NULL, IMMUTABLE);
    }


    @Override
    public void write(Kryo kryo, Output output, Map<Object, Object> immutableMap) {
        kryo.writeObject(output, new HashMap<>(immutableMap));
    }

    @Override
    public Map<Object, Object> read(Kryo kryo, Input input, Class<Map<Object, Object>> type) {
        Map map = kryo.readObject(input, HashMap.class);
        Set<Map.Entry<Object, Object>> entries = map.entrySet();
        Map.Entry<Object, Object>[] entriesArray = entries.toArray(new Map.Entry[entries.size()]);
        return Map.ofEntries(entriesArray);
    }

    /**
     * Creates a new {@link de.javakaffee.kryoserializers.guava.ImmutableMapSerializer} and registers its serializer
     * for the several ImmutableMap related classes.
     *
     * @param kryo the {@link Kryo} instance to set the serializer on
     */
    public static void registerSerializers(final Kryo kryo) {

       final Java9ImmutableMapSerializer serializer = new Java9ImmutableMapSerializer();

        Object key1 = new Object();
        Object key2 = new Object();
        Object key3 = new Object();
        Object key4 = new Object();
        Object key5 = new Object();
        Object key6 = new Object();
        Object key7 = new Object();
        Object key8 = new Object();
        Object key9 = new Object();
        Object key10 = new Object();
        Object value = new Object();


        kryo.register(Map.of(key1, value).getClass(), serializer);
        kryo.register(Map.of(key1, value, key2, value).getClass(), serializer);
        kryo.register(Map.of(key1, value, key2, value, key3, value).getClass(), serializer);
        kryo.register(Map.of(key1, value, key2, value, key3, value, key4, value).getClass(), serializer);
        kryo.register(Map.of(key1, value, key2, value, key3, value, key4, value, key5, value).getClass(), serializer);
        kryo.register(Map.of(key1, value, key2, value, key3, value, key4, value, key5, value, key6, value).getClass(), serializer);
        kryo.register(Map.of(key1, value, key2, value, key3, value, key4, value, key5, value, key6, value, key7, value).getClass(), serializer);
        kryo.register(Map.of(key1, value, key2, value, key3, value, key4, value, key5, value, key6, value, key7, value, key8, value).getClass(), serializer);
        kryo.register(Map.of(key1, value, key2, value, key3, value, key4, value, key5, value, key6, value, key7, value, key8, value, key9, value).getClass(), serializer);
        kryo.register(Map.of(key1, value, key2, value, key3, value, key4, value, key5, value, key6, value, key7, value, key8, value, key9, value, key10, value).getClass(), serializer);
        kryo.register(Map.ofEntries(Map.entry(key1, value)).getClass(), serializer);

    }
}