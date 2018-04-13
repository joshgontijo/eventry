package io.joshworks.fstore.es;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.VStringSerializer;

import java.nio.ByteBuffer;
import java.util.UUID;

public class User {
    private final String name;
    private final int age;

    public static User rand() {
        String rand = UUID.randomUUID().toString();
        return new User(rand, 0);
    }

    public User(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("User{");
        sb.append("name='").append(name).append('\'');
        sb.append(", age=").append(age);
        sb.append('}');
        return sb.toString();
    }

    private static final VStringSerializer stringSerializer = new VStringSerializer();

    public static Serializer<User> userSerializer() {
        return new Serializer<User>() {
            @Override
            public ByteBuffer toBytes(User data) {
                ByteBuffer name = stringSerializer.toBytes(data.name);
                ByteBuffer bb = ByteBuffer.allocate(name.capacity() + Integer.BYTES);
                return (ByteBuffer) bb.put(name).putInt(data.age).flip();
            }

            @Override
            public User fromBytes(ByteBuffer buffer) {
                String name = stringSerializer.fromBytes(buffer);
                int age = buffer.getInt();
                return new User( name, age);
            }
        };
    }


}
