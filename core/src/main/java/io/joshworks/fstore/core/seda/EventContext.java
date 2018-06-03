package io.joshworks.fstore.core.seda;

public class EventContext<T> {

    public final String correlationId;
    public final T data;
    private SedaContext sedaContext;

    EventContext(String correlationId, T data, SedaContext sedaContext) {
        this.correlationId = correlationId;
        this.data = data;
        this.sedaContext = sedaContext;
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
        sedaContext.sendToNextStage(stageName, correlationId, event);
    }
}
