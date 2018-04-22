package io.joshworks.fstore.es;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class Main {


    public static void main(String[] args) throws IOException {

        File file = new File("J:\\event-db");
        if (Files.exists(file.toPath())) {
            Files.list(file.toPath()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            Files.deleteIfExists(file.toPath());

        }

        EventStore store = EventStore.open(file);
//        EventStore2 store = EventStore2.open(new File("event-db"));


        long start = System.currentTimeMillis();
        int size = 1000000;
        for (int i = 0; i < size; i++) {
            store.put("test-stream-" + i, new Event("" + i, "data-" + i, System.currentTimeMillis()));
        }
        System.out.println("WRITE: " + (System.currentTimeMillis() - start));


        start = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            List<Event> events = store.get("test-stream-" + i);
            if (events.size() != 1) {
                store.close();
                throw new RuntimeException("Not equals to one, idx: " + i);
            }
        }
        System.out.println("READ: " + (System.currentTimeMillis() - start));
//        System.out.println("READ: " + store.stream().count());

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
