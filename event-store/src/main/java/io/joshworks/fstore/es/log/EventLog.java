//package io.joshworks.fstore.es.log;
//
//import io.joshworks.fstore.es.index.IndexEntry;
//import io.joshworks.fstore.log.appender.LogAppender;
//
//import java.io.Closeable;
//import java.io.File;
//import java.util.Iterator;
//import java.util.stream.Stream;
//
//public class EventLog implements Closeable {
//
//    private final LogAppender<Event> appender;
//
//    public EventLog(LogAppender<Event> appender) {
//        this.appender = appender;
//    }
//
//    public long add(Event event) {
//        return appender.append(event);
//    }
//
//    public Event get(String stream, IndexEntry key) {
//        Event event = appender.get(key.position);
//        if(event == null) {
//            throw new IllegalArgumentException("No event found for " + key);
//        }
//        event.stream(stream);
//        event.version(key.version);
//        return event;
//    }
//
//    public String roll() {
//        String segmentName = appender.currentSegment();
//        appender.roll();
//        return segmentName;
//    }
//
//    public File directory() {
//        return appender.directory().toFile();
//    }
//
//    @Override
//    public void close()  {
//        appender.close();
//    }
//
//    public Iterator<Event> iterator() {
//        return appender.scanner();
//    }
//
//    public Stream<Event> stream() {
//        return appender.stream();
//    }
//}
