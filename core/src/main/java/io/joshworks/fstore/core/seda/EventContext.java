package io.joshworks.fstore.core.seda;

import java.util.concurrent.CompletableFuture;

public class EventContext<T> {

    public final String correlationId;
    public final T data;
    private SedaContext sedaContext;
    private final CompletableFuture<Object> future;

    EventContext(String correlationId, T data, SedaContext sedaContext, CompletableFuture<Object> future) {
        this.correlationId = correlationId;
        this.data = data;
        this.sedaContext = sedaContext;
        this.future = future;
    }

    /**
     * Enqueue this message to the next stage, if the blockWhenFull is used by the downstream stage,
     * then this producer will block until the queue is available, otherwise an {@link EnqueueException} is thrown
     *
     * @param event     The event to be enqueued
     * @param stageName The stage name to submit this event
     * @throws EnqueueException if the queue is full and blockWhenFull is not set
     */
    public void submit(String stageName, Object event) {
        sedaContext.sendToNextStage(stageName, correlationId, event, future);
    }

    public void complete(Object value) {
        future.complete(value);
    }
}
