package io.joshworks.fstore.es;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.File;
import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) throws ScriptException, NoSuchMethodException {
        EventStore store = EventStore.open(new File("event-db"));
        store.put("yolo", new Event("AA", "yolo1"));
        store.put("yolo", new Event("BB", "yolo2"));
        store.put("josh", new Event("CC", "josh1"));
        store.put("josh", new Event("DD", "josh2"));

        List<Event> yolos = store.get("yolo");
        System.out.println(Arrays.toString(yolos.toArray(new Event[yolos.size()])));

        List<Event> joshs = store.get("josh");
        System.out.println(Arrays.toString(joshs.toArray(new Event[joshs.size()])));

        List<Event> joshsAfter0 = store.get("josh", 1);
        System.out.println(Arrays.toString(joshsAfter0.toArray(new Event[joshsAfter0.size()])));




        ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
        engine.eval("var eventHandler = function(event) {" +
                "    print(event);" +
                "    return event.type" +
                "};");



        Event item = store.get("josh", 1).get(0);

        Invocable invocable = (Invocable) engine;

        Object result = invocable.invokeFunction("eventHandler", item);
        System.out.println(result);
        System.out.println(result.getClass());


        store.close();


    }

}
