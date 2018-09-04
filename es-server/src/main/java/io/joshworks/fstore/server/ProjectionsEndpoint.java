package io.joshworks.fstore.server;

import io.joshworks.fstore.es.EventStore;
import io.joshworks.fstore.es.projections.Script;
import io.joshworks.snappy.http.HttpException;
import io.joshworks.snappy.http.HttpExchange;

import javax.script.ScriptException;
import java.util.HashMap;
import java.util.Map;

public class ProjectionsEndpoint {

    private static final Map<String, Projection> projections = new HashMap<>();

    private final EventStore store;

    public ProjectionsEndpoint(EventStore store) {
        this.store = store;
    }

    public void create(HttpExchange exchange) {
        String name = exchange.pathParameter("name");
        String script = exchange.body().asString();

        //TODO use json, this is just send the js easily
        Projection projection = new Projection();
        projection.script = script;

        projections.put(name, projection);
        exchange.status(201);
    }

    public void run(HttpExchange exchange) {
        String name = exchange.pathParameter("name");
        Projection projection = projections.get(name);

        if(projection == null) {
            exchange.status(404).end();
            return;
        }

        Script script = new Script(store);
        try {
            script.run(projection.script);
        } catch (ScriptException e) {
            throw new HttpException(500, e.getMessage());
        }
    }

    public void getAll(HttpExchange exchange) {
        exchange.send(projections);
    }

    public void get(HttpExchange exchange) {
        String name = exchange.pathParameter("name");
        Projection projection = projections.get(name);
        if(projection == null) {
            exchange.status(404).end();
            return;
        }
        exchange.send(projection);
    }

    private static class Projection {
        private String script;
    }

}
