package io.joshworks.fstore.core.eventbus;

public class Context {

    public final String method;
    public final Object arg;
    public final Object handler;


    Context(String method, Object arg, Object handler) {
        this.method = method;
        this.arg = arg;
        this.handler = handler;
    }

    @Override
    public String toString() {
        return handler == null ? "" : handler.getClass().getSimpleName() + "#" +
                method + "(" + String.valueOf(arg) + ")";
    }
}
