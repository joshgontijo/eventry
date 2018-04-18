package io.joshworks.fstore.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.GregorianCalendarSerializer;
import de.javakaffee.kryoserializers.JdkProxySerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import io.joshworks.fstore.core.Serializer;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationHandler;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;

public class KryoStoreSerializer<T> implements Serializer<T> {

    private final Kryo kryo;
    private final Class<T> type;

    public KryoStoreSerializer(Class<T> type) {
        this.type = type;
        kryo = newKryoInstance(type);
    }

    private static Kryo newKryoInstance(Class<?> type) {
        Kryo kryo = new Kryo();
        kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        kryo.register(type);
        kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
        kryo.register(Collections.EMPTY_LIST.getClass(), new DefaultSerializers.CollectionsEmptyListSerializer());
        kryo.register(Collections.EMPTY_MAP.getClass(), new DefaultSerializers.CollectionsEmptyMapSerializer());
        kryo.register(Collections.EMPTY_SET.getClass(), new DefaultSerializers.CollectionsEmptySetSerializer());
        kryo.register(Collections.singletonList("").getClass(), new DefaultSerializers.CollectionsSingletonListSerializer());
        kryo.register(Collections.singleton("").getClass(), new DefaultSerializers.CollectionsSingletonSetSerializer());
        kryo.register(Collections.singletonMap("", "").getClass(), new DefaultSerializers.CollectionsSingletonMapSerializer());
        kryo.register(GregorianCalendar.class, new GregorianCalendarSerializer());
        kryo.register(InvocationHandler.class, new JdkProxySerializer());
        UnmodifiableCollectionsSerializer.registerSerializers(kryo);
        SynchronizedCollectionsSerializer.registerSerializers(kryo);
        Java9ImmutableMapSerializer.registerSerializers(kryo);
        return kryo;
    }

    @Override
    public ByteBuffer toBytes(T data) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try(Output output = new Output(baos)) {
            kryo.writeObject(output, data);
        }
        return ByteBuffer.wrap(baos.toByteArray());
    }

    @Override
    public void writeTo(T data, ByteBuffer dest) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try(Output output = new Output(baos)) {
            kryo.writeObject(output, data);
        }
        dest.put(baos.toByteArray());
    }

    @Override
    public T fromBytes(ByteBuffer data) {
        return kryo.readObject(new Input(data.array()), type);
    }


}
