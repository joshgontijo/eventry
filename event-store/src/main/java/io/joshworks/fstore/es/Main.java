package io.joshworks.fstore.es;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.UUID;

public class Main {


    public static void main(String[] args) throws IOException {

        File file = new File("J:\\event-db\\" + UUID.randomUUID().toString().substring(0,8));
        Files.createDirectories(file.toPath());

        EventStore store = EventStore.open(file);

        long start = System.currentTimeMillis();
        int size = 1000000;
        for (int i = 0; i < size; i++) {
            store.put("test-stream-" + i, Event.create("" + i, "data-" + i));
        }
        System.out.println("WRITE: " + (System.currentTimeMillis() - start));


        start = System.currentTimeMillis();

        //TODO returning 1000002 entries instead of 1000000
//        List<Event> events = store.get("test-stream-1");
//        System.out.println(events.size());

        for (int i = 0; i < size; i++) {
            List<Event> events = store.get("test-stream-" + i);
            if (events.size() != 1) {
                store.close();
                throw new RuntimeException("Not equals to one: "+events.size()+" idx: " + i);
            }
        }
        System.out.println("READ: " + (System.currentTimeMillis() - start));


        store.report();

//        System.out.println("READ: " + store.stream().count());

//        for (int i = 0; i < position; i++) {
//            store.put("test-stream", new Event("" + i, "data-" + i));
//        }


//        store.put("yolo", new Event("AA", "yolo1"));
//        store.put("yolo", new Event("BB", "yolo2"));
//        store.put("josh", new Event("CC", "josh1"));
//        store.put("josh", new Event("DD", "josh2"));
//
//        List<Event> yolos = store.get("yolo");
//        System.out.println(Arrays.toString(yolos.toArray(new Event[yolos.position()])));
//
//        List<Event> joshs = store.get("josh");
//        System.out.println(Arrays.toString(joshs.toArray(new Event[joshs.position()])));
//
//        List<Event> joshsAfter0 = store.get("josh", 1);
//        System.out.println(Arrays.toString(joshsAfter0.toArray(new Event[joshsAfter0.position()])));


        store.close();


//        store.close();


    }

}
