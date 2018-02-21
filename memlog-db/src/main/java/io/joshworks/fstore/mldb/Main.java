package io.joshworks.fstore.mldb;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.VStringSerializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class Main {

    public static void main(String[] args) {
        Store<String, User> store = Store.create(new File("memStore"), JsonSerializer.of(User.class), User::getId);

        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            store.put(User.rand());
        }
        System.out.println("Insert: " + (System.currentTimeMillis() - start) + "ms");

        Iterator<User> iterator = store.iterator(IteradorOrder.INSERT);
        start = System.currentTimeMillis();
        while(iterator.hasNext()) {
            User next = iterator.next();
        }
        System.out.println("READ INSERT ORDER: " + (System.currentTimeMillis() - start) + "ms");

        iterator = store.iterator(IteradorOrder.KEY);
        start = System.currentTimeMillis();
        while(iterator.hasNext()) {
            User next = iterator.next();
        }
        System.out.println("READ KEY ORDER: " + (System.currentTimeMillis() - start) + "ms");
    }




}

class User {
    private final String id;
    private final String name;
    private final int age;

    public static User rand() {
        String rand = UUID.randomUUID().toString();
        int randAge = ThreadLocalRandom.current().nextInt(0, 100);
        return new User(rand, "name_" + rand, randAge);
    }

    public User(String id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User user = (User) o;
        return age == user.age &&
                Objects.equals(id, user.id) &&
                Objects.equals(name, user.name);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, name, age);
    }

    private static final VStringSerializer stringSerializer = new VStringSerializer();

    public static Serializer<User> userSerializer() {
        return new Serializer<User>() {
            @Override
            public ByteBuffer toBytes(User data) {
                ByteBuffer id = stringSerializer.toBytes(data.id);
                ByteBuffer name = stringSerializer.toBytes(data.name);
                ByteBuffer bb = ByteBuffer.allocate(id.capacity() + name.capacity() + Integer.BYTES);
                return (ByteBuffer) bb.put(id).put(name).putInt(data.age).flip();
            }

            @Override
            public User fromBytes(ByteBuffer buffer) {
                String id = stringSerializer.fromBytes(buffer);
                String name = stringSerializer.fromBytes(buffer);
                int age = buffer.getInt();
                return new User(id, name, age);
            }
        };
    }


}