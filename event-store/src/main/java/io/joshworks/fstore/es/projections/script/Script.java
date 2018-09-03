package io.joshworks.fstore.es.projections.script;

import io.joshworks.fstore.es.EventStore;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class Script {

    private final ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
    private final ScriptAPI api;
    private EventStore store;

    private final Map<String, Object> state = new HashMap<>();
    private Map<String, Object> options = new HashMap<>();

    public Script(EventStore store) {
        this.store = store;
        this.api = new ScriptAPI(store);

        engine.put("state", state);
        engine.put("options", (Consumer<Map<String, Object>>) this::options);

        engine.put("fromStream", api.fromStream);
        engine.put("fromStreams", api.fromStreams);
        engine.put("foreachStream", api.fromStreams);
        engine.put("emit", api.emit);
        engine.put("linkTo", api.linkTo);
    }

    public void options(Map<String, Object> options) {
        this.options = options;
    }

    public void run(String script) throws ScriptException {
        engine.eval(script);

        if(options.containsKey("persistState")) {
            System.out.println("TODO persist state");
        }

        System.out.println(Arrays.toString(state.entrySet().toArray()));
        System.out.println(Arrays.toString(options.entrySet().toArray()));

    }



//    public static void main(String[] args) throws Exception {
//        Gson gson = new Gson();
//        EventStore store = EventStore.open(new File("J:\\event-store-app2"));
//        store.add(Event.create("stream1", "type", ""))
//
//        new Script(store).run2(new InputStreamReader(Script.class.getClassLoader().getResourceAsStream("script.js")));
//    }



}
