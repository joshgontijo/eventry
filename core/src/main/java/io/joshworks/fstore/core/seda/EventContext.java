package io.joshworks.fstore.core.seda;

public class EventContext<T> implements Publisher {

    public final String correlationId;
    public final T data;
    private final SedaContext context;

    EventContext(String correlationId, T data, SedaContext context) {
        this.correlationId = correlationId;
        this.data = data;
        this.context = context;
    }

    @Override
    public void publish(Object event) {
        publish(this.correlationId, event);
    }

    @Override
    public void publish(String correlationId, Object event) {
        context.sendToNextStage(correlationId, event);
    }

}
