package io.joshworks.fstore;

import io.joshworks.fstore.serializer.EventSerializer;
import io.joshworks.fstore.serializer.JsonSerializer;
import io.joshworks.fstore.event.Event;
import io.joshworks.fstore.serializer.KryoEventSerializer;
import io.joshworks.fstore.store.RandomAccessDataStore;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

public class Main {

    public static void main(String[] args) {

//        System.out.println(MurmurHash.hash64("hello"));
//        System.out.println(MurmurHash.hash64("josueeduardogontijoi"));
//        System.out.println(MurmurHash.hash64("world"));
//
        EventStore store = new EventStore(new RandomAccessDataStore("1M.dat"), new JsonSerializer());
//
//        Event event1 = Event.allocate("stream-123", Map.allocate("k1", "v1"), "TEST-EVENT-1", 1, System.currentTimeMillis());
//        Position position1 = store.save(event1);
//        Event found1 = store.get(position1);
//        System.out.println(found1);
//
//        Event event2 = Event.allocate("stream-456", Map.allocate("k1", "v1"), "TEST-EVENT-2", 2, System.currentTimeMillis());
//        Position position2 = store.save(event2);
//        Event found2 = store.get(position2);
//        System.out.println(found2);

//        insertN(store, 100000);

        EventSerializer serializer = new KryoEventSerializer();
        byte[] bytes = serializer.toBytes(Event.of("stream-123", Map.of("k1", "v1"), "TEST-EVENT-1", 1, System.currentTimeMillis()));
        System.out.println(bytes.length);

        serializer = new JsonSerializer();
        bytes = serializer.toBytes(Event.of("stream-123", Map.of("k1", "v1"), "TEST-EVENT-1", 1, System.currentTimeMillis()));
        System.out.println(bytes.length);

    }

    public static void insertN(EventStore store, long number) {
        long start = System.currentTimeMillis();
        BigDecimal avg = new BigDecimal(0);
        for (int i = 0; i < number; i++) {
            String uuid = UUID.randomUUID().toString();
            String someRand = uuid.substring(0, 8);
            Event event = Event.create(uuid, Map.of("k1", someRand), someRand);
            long startSingle = System.currentTimeMillis();
            store.save(event);
            avg = avg.add(BigDecimal.valueOf(System.currentTimeMillis() - startSingle));
        }
        long end = System.currentTimeMillis();
        System.out.println("TOTAL: " + (end - start) + "ms");
        System.out.println("AVG TIME: " + avg.divide(BigDecimal.valueOf(number)) + "ms");

    }

}
