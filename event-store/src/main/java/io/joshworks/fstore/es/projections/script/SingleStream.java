package io.joshworks.fstore.es.projections.script;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class SingleStream {

    private final Stream<JsonEvent> stream;

    public SingleStream(Stream<JsonEvent> stream) {
        this.stream = stream;
    }

    public void forEach(Consumer<? super JsonEvent> handler) {
        stream.forEach(handler);
    }

    public void when(ScriptObjectMirror handlers) {
        stream.forEach(event -> {
            if (handlers.containsKey(event.type)) {
                handlers.callMember(event.type, event);
            }
            if (handlers.containsKey("_any")) {
                handlers.callMember(event.type, event);
            }

        });
    }

    public SingleStream filter(Predicate<? super JsonEvent> filter) {
        return new SingleStream(stream.filter(filter));
    }
}
