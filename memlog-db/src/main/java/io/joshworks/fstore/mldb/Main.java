package io.joshworks.fstore.mldb;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.VStringSerializer;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class Main {

    public static void main(String[] args) {
        Store<String, User> store = Store.create(new File("memStore"), User.userSerializer(), User::getId);

        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            store.put(User.with(String.valueOf(i)));
        }
        System.out.println("INSERT: " + (System.currentTimeMillis() - start) + "ms");

        Iterator<User> iterator = store.iterator();
        start = System.currentTimeMillis();
        while(iterator.hasNext()) {
            User next = iterator.next();

        }
        System.out.println("READ_KEY_ORDER: " + (System.currentTimeMillis() - start) + "ms");

    }




}

class User {
    private final String id;
    private final String name;
    private final int age;

    public static User rand() {
        String rand = UUID.randomUUID().toString();
        return with(rand);
    }

    public static User with(String id) {
        int randAge = ThreadLocalRandom.current().nextInt(0, 100);
        return new User(id, "name_" + id, randAge);
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

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("User{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", age=").append(age);
        sb.append('}');
        return sb.toString();
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