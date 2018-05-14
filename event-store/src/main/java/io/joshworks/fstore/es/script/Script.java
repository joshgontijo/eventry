package io.joshworks.fstore.es.script;

import io.joshworks.fstore.es.EventStore;
import io.joshworks.fstore.es.log.Event;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class Script {

    private final ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
    private EventStore store;

    public Script(EventStore store) throws ScriptException {
        this.store = store;
        engine.eval(new InputStreamReader(Script.class.getClassLoader().getResourceAsStream("script.js")));
    }

    public void run() throws ScriptException, NoSuchMethodException {
        Invocable invocable = (Invocable) engine;


        Map<String, Object> data = new HashMap<>();

        Object result = invocable.invokeFunction("main", store);
        System.out.println(result);
        System.out.println(result.getClass());
        System.out.println(data);
    }

    public static void fromStream(EventStore store, String stream, BiConsumer<Event, Map<String, Object>> consumer) {

//        store.fromStream(stream).

    }

    public static void main(String[] args) throws Exception {
        new Script(null).run();
    }



}
