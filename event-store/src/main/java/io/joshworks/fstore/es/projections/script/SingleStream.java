package io.joshworks.fstore.es.projections.script;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class SingleStream {

    private final Stream<JsonEvent> stream;
    private Map<String, Object> state = new HashMap<>();

    public SingleStream(Stream<JsonEvent> stream) {
        this.stream = stream;
    }

    public SingleStream withState(Map<String, Object> state) {
        this.state = state;
        return this;
    }

    public SingleStream filter(Predicate<? super JsonEvent> filter) {
        return new SingleStream(stream.filter(filter));
    }

    public SingleStream forEach(BiConsumer<Map<String, Object>, ? super JsonEvent> handler) {
        stream.forEach(event -> handler.accept(state, event));
        return this;
    }

    public SingleStream when(ScriptObjectMirror handlers) {
        stream.forEach(event -> {
            if (handlers.containsKey(event.type)) {
                handlers.callMember(event.type, state, event);
            }
            if (handlers.containsKey("_any")) {
                handlers.callMember(event.type, state, event);
            }
        });
        return this;
    }

    public SingleStream persistState() {
        //TODO
        System.out.println("Persisting state");
        System.out.println(Arrays.toString(state.entrySet().toArray()));
        return this;
    }

}


