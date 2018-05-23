package io.joshworks.fstore.core.seda;

public class EventContext<T> implements Publisher{

    public final String correlationId;
    public final T data;
    private final LocalSedaContext context;

    EventContext(String correlationId, T data, LocalSedaContext context) {
        this.correlationId = correlationId;
        this.data = data;
        this.context = context;
    }

    @Override
    public void submit(Object event) {
        context.submit(this.correlationId, event);
    }

    @Override
    public void submit(String correlationId, Object event) {
        context.submit(correlationId, event);
    }

}
