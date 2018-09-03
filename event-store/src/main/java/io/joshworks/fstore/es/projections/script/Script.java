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

    private Map<String, Object> options = new HashMap<>();

    public Script(EventStore store) {
        this.store = store;
        this.api = new ScriptAPI(store);

        engine.put("options", (Consumer<Map<String, Object>>) this::options);

        engine.put("fromStream", api.fromStream);
        engine.put("fromStreams", api.fromStreams);
        engine.put("foreachStream", api.foreachstream);

        engine.put("emit", api.emit);
        engine.put("linkTo", api.linkTo);
    }

    public void options(Map<String, Object> options) {
        this.options = options;
    }

    public void run(String script) throws ScriptException {
        engine.eval(script);

        System.out.println(Arrays.toString(options.entrySet().toArray()));

    }



}
