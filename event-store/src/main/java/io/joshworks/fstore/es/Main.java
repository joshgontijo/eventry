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
            store.put("test-stream-", Event.create("" + i, "data-" + i));
        }
        System.out.println("WRITE: " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();

        for (int i = 0; i < size; i++) {
            List<Event> events = store.get("test-stream-");
//            if (events.size() != 1) {
//                store.close();
//                throw new RuntimeException("Not equals to one: "+events.size()+" idx: " + i);
//            }
        }
        System.out.println("READ: " + (System.currentTimeMillis() - start));


        store.close();


    }

}
