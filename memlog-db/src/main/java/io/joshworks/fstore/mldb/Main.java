package io.joshworks.fstore.mldb;

import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

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

        int size = 1000000;

        long start = System.currentTimeMillis();
        try (Store<Integer, Integer> store = Store.open(new File("memStore"), Serializers.INTEGER, Serializers.INTEGER)) {
            for (int i = 0; i < size; i++) {
                store.put(i, 1);
            }
        }

        System.out.println("WRITE: " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();

        int counter = 0;
        try (Store<Integer, Integer> store = Store.open(new File("memStore"), Serializers.INTEGER, Serializers.INTEGER)) {
            Iterator<Integer> iterator = store.iterator();

            while (iterator.hasNext()) {
                Integer next = iterator.next();
                counter++;
            }
        }

        if(counter != size) {
            throw new RuntimeException("WRITTEN ENTRIES: " + size + " - READ: " + counter);
        }

        System.out.println("READ: " + counter + " entries in " + (System.currentTimeMillis() - start));



    }


}

