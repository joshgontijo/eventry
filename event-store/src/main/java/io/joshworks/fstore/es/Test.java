package io.joshworks.fstore.es;

import com.google.gson.Gson;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Test {

    public static void main(String[] args) throws IOException {


        File dir = new File("J:\\fxdata");
        Files.createDirectories(dir.toPath());

        try (EventStore store = EventStore.open(dir)) {

//            try (BufferedReader br = new BufferedReader(new InputStreamReader(Test.class.getClassLoader().getResourceAsStream("EURUSD-Hour.csv")))) {
//                String line;
//                while ((line = br.readLine()) != null) {
//                    Entry entry = Entry.parse(line);
//                    store.add("EURUSD", Event.create("candle", entry.bytes()));
//                }
//            }

//            store.fromStream("EURUSD").forEach(e -> {
//                Entry entry = Entry.fromBytes(e.data());
//                store.linkTo("hour-of-day-" + entry.time.getHour(), e);
//                store.linkTo("day-of-month-" + entry.time.getDayOfMonth(), e);
//                store.linkTo("day-of-week-" + entry.time.getDayOfWeek(), e);
//
//            });

//            store.fromStream("hour-of-day-15").map(Event::map).forEach(System.out::println);

//            Stream<Entry> entryStream = store.fromStream("hour-of-day-15").map(e -> Entry.fromBytes(e.data()));

//            entryStream.reduce(0, )


//            store.fromStream("hour-of-day-15").map(Event::map).forEach(System.out::println);
//
//            for (int i = 0; i < 5; i++) {
//                store.fromStream("hour-of-day-15").map(Event::map).forEach(System.out::println);
//            }
//
            final Map<String, AtomicInteger> result = new HashMap<>();
            AtomicInteger total = new AtomicInteger();
            stream(store, "day-of-week-TUESDAY").forEach(e -> {
                String up = e.time.getHour() + "-UP";
                String down = e.time.getHour() + "-DOWN";
                result.putIfAbsent(up, new AtomicInteger());
                result.putIfAbsent(down, new AtomicInteger());

                if(e.var() > 0)
                    result.get(up).incrementAndGet();
                else
                    result.get(down).incrementAndGet();

                total.incrementAndGet();
            });

            for (Map.Entry<String, AtomicInteger> entry : result.entrySet()) {
                System.out.println(entry.getKey() + ": " + (entry.getValue().get()));
            }


        }

    }


    private static Stream<Entry> stream(EventStore store, String stream) {
        return store.fromStream(stream).map(e -> Entry.fromBytes(e.data()));
    }


    private static class Entry {

        private static final Gson gson = new Gson();
        private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

        public final LocalDateTime time;

        public final double open;
        public final double close;
        public final double high;
        public final double low;

        private Entry(LocalDateTime time, double open, double close, double high, double low) {
            this.time = time;
            this.open = open;
            this.close = close;
            this.high = high;
            this.low = low;
        }

        public static Entry parse(String line) {
            String[] split = line.split(",");
            LocalDateTime time = LocalDateTime.parse(split[0], formatter);
            double open = Double.parseDouble(split[1]);
            double close = Double.parseDouble(split[2]);
            double high = Double.parseDouble(split[3]);
            double low = Double.parseDouble(split[4]);

            return new Entry(time, open, close, high, low);
        }

        public double var() {
            return open - close;
        }

        public double varP() {
            return (var() / open) * 100;
        }


        public byte[] bytes() {
            return gson.toJson(this).getBytes(StandardCharsets.UTF_8);
        }

        public static Entry fromBytes(byte[] bytes) {
            return gson.fromJson(new InputStreamReader(new ByteArrayInputStream(bytes)), Entry.class);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("Entry{");
            sb.append("time=").append(time);
            sb.append(", create=").append(open);
            sb.append(", close=").append(close);
            sb.append(", high=").append(high);
            sb.append(", low=").append(low);
            sb.append('}');
            return sb.toString();
        }
    }

}
