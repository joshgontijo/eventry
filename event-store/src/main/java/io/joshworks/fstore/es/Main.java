package io.joshworks.fstore.es;

import java.io.File;

public class Main {


    public static void main(String[] args) {


        EventStore store = EventStore.open(new File("event-db"));
//        EventStore2 store = EventStore2.open(new File("event-db"));


        long start = System.currentTimeMillis();
        int size = 1000000;
        for (int i = 0; i < size; i++) {
            store.put("test-stream-", new Event("" + i, "data-" + i, System.currentTimeMillis()));
        }
        System.out.println("WRITE: " + (System.currentTimeMillis() - start));


        System.out.println("READ: " + store.stream().count());

//        for (int i = 0; i < size; i++) {
//            store.put("test-stream", new Event("" + i, "data-" + i));
//        }



//        store.put("yolo", new Event("AA", "yolo1"));
//        store.put("yolo", new Event("BB", "yolo2"));
//        store.put("josh", new Event("CC", "josh1"));
//        store.put("josh", new Event("DD", "josh2"));
//
//        List<Event> yolos = store.get("yolo");
//        System.out.println(Arrays.toString(yolos.toArray(new Event[yolos.size()])));
//
//        List<Event> joshs = store.get("josh");
//        System.out.println(Arrays.toString(joshs.toArray(new Event[joshs.size()])));
//
//        List<Event> joshsAfter0 = store.get("josh", 1);
//        System.out.println(Arrays.toString(joshsAfter0.toArray(new Event[joshsAfter0.size()])));


        store.close();





//        store.close();


    }

}
