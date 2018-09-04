package io.joshworks.fstore.es.projections;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public class ForEachStream {

    private final Map<String, Stream<JsonEvent>> streams;
    private Map<String, Object> original = new HashMap<>();
    private Map<String, Map<String, Object>> streamState = new HashMap<>();

    private final ExecutorService executor = Executors.newFixedThreadPool(5);


    public ForEachStream(Map<String, Stream<JsonEvent>>  streams) {
        this.streams = streams;
    }

    public ForEachStream withState(Map<String, Object> state) {
        this.original = state;
        return this;
    }

    public ForEachStream when(ScriptObjectMirror handlers) {
        streams.forEach((key, value) -> {
            streamState.putIfAbsent(key, new HashMap<>(original));
            executor.submit(() -> {
                value.forEach(event -> {
                    if (handlers.containsKey(event.type)) {
                        handlers.callMember(event.type, streamState, event);
                    }
                    if (handlers.containsKey("_any")) {
                        handlers.callMember(event.type, streamState, event);
                    }
                });
            });
        });
        return this;
    }

    public ForEachStream persistState() {
        //TODO
        System.out.println("Persisting stream states");
        streamState.forEach((k,v) -> {
            System.out.println(Arrays.toString(v.entrySet().toArray()));
        });

        return this;
    }

}
