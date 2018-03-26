package io.joshworks.fstore.mldb;

import io.joshworks.fstore.serializer.StandardSerializer;

import java.io.File;
import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
//        Store<String, User> store = Store.create(new File("memStore"), User.userSerializer(), User::getId);
//
//        long start = System.currentTimeMillis();
//        for (int i = 0; i < 1000000; i++) {
//            store.put(User.with(String.valueOf(i)));
//        }
//        System.out.println("INSERT: " + (System.currentTimeMillis() - start) + "ms");
//
//        Iterator<User> iterator = store.iterator();
//        start = System.currentTimeMillis();
//        while(iterator.hasNext()) {
//            User next = iterator.next();
//
//        }
//        System.out.println("READ_KEY_ORDER: " + (System.currentTimeMillis() - start) + "ms");


        for (int i = 0; i < 10; i++) {
        Store<Integer, User> store = Store.open(new File("memStore"), StandardSerializer.of(Integer.class), User.userSerializer());
            store.put(1, new User("John", 10));
            store.put(2, new User("Billy", 20));
            store.put(3, new User("Josh", 30));


            store.delete(2);

            store.close();

            store = Store.open(new File("memStore"), StandardSerializer.of(Integer.class), User.userSerializer());
            store.iterator().forEachRemaining(System.out::println);

        }


    }


}

