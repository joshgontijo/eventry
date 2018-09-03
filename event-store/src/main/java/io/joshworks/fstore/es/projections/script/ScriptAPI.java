package io.joshworks.fstore.es.projections.script;

import io.joshworks.fstore.es.EventStore;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class ScriptAPI {

    final EventStore store;
    final Function<String, SingleStream> fromStream;
    final Function<String[], SingleStream> fromStreams;
//    final Function<String[], SingleStream> foreachstream;
    final BiConsumer<String, JsonEvent> linkTo;
    final BiConsumer<String, JsonEvent> emit;

    public ScriptAPI(EventStore store) {
        this.store = store;
        this.fromStream = s -> new SingleStream(store.fromStream(s).map(JsonEvent::from));
        this.fromStreams = streams ->  new SingleStream(store.zipStreams( Set.of(streams)).map(JsonEvent::from));
//        this.foreachstream = streams -> {
//            Set<String> streams1 = Set.of(streams);
//            Map<String, Stream<Event>> mapped = store.fromStreamsMapped(streams1);
//            return mapped.entrySet().stream()
//                    .map(kv -> new AbstractMap.SimpleEntry<>(kv.getKey(), kv.getValue().map(JsonEvent::from)))
//                    .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
//        };

        this.linkTo = (stream, event) -> store.linkTo(stream, event.toEvent());
        this.emit = (stream, event) -> store.emit(stream, event.toEvent());
    }

    //    public static class ForEachStream {
//
//        private final Stream<JsonEvent> stream;
//
//        public ForEachStream(Stream<JsonEvent> stream) {
//            this.stream = stream;
//        }
//
//        public void forEach(Consumer<? super JsonEvent> handler) {
//            stream.forEach(handler);
//        }
//
//        public void when(Map<String, Consumer<? super JsonEvent>> handlers) {
//            var anyHandler = handlers.getOrDefault("_any", e ->{});
//            stream.forEach(event -> handlers.getOrDefault(event.type, anyHandler).accept(event));
//        }
//
//        public SingleStream filter(Predicate<? super JsonEvent> filter) {
//            return new SingleStream(stream.filter(filter));
//        }
//    }


}
