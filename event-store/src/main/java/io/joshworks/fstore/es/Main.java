//package io.joshworks.fstore.es;
//
//import io.joshworks.fstore.serializer.StandardSerializer;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.Iterator;
//
//@SuppressWarnings("Duplicates")
//public class Main {
//
//    public static void main(String[] args) throws IOException {
////        Store<String, User> store = Store.create(new File("memStore"), User.userSerializer(), User::getId);
////
////        long start = System.currentTimeMillis();
////        for (int i = 0; i < 1000000; i++) {
////            store.put(User.with(String.valueOf(i)));
////        }
////        System.out.println("INSERT: " + (System.currentTimeMillis() - start) + "ms");
////
////        Iterator<User> iterator = store.iterator();
////        start = System.currentTimeMillis();
////        while(iterator.hasNext()) {
////            User next = iterator.next();
////
////        }
////        System.out.println("READ_KEY_ORDER: " + (System.currentTimeMillis() - start) + "ms");
//
//
//        long start = System.currentTimeMillis();
//        try (EventStore<Integer, Integer> eventStore = EventStore.open(new File("memStore"), StandardSerializer.INTEGER, StandardSerializer.INTEGER)) {
//            for (int i = 0; i < 1000000; i++) {
//                eventStore.put(i, 1);
//            }
//        }
//
//        System.out.println("WRITE: " + (System.currentTimeMillis() - start));
//
//        start = System.currentTimeMillis();
//        try (EventStore<Integer, Integer> eventStore = EventStore.open(new File("memStore"), StandardSerializer.INTEGER, StandardSerializer.INTEGER)) {
//            Iterator<Integer> iterator = eventStore.iterator();
//
//            int idx = 0;
//            while (iterator.hasNext()) {
//                Integer next = iterator.next();
//
////                System.out.println("KEY: " + idx++);
//            }
//
//        }
//        System.out.println("READ: " + (System.currentTimeMillis() - start));
//
//
//
//    }
//
//
//}
//
