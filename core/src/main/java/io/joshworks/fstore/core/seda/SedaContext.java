package io.joshworks.fstore.core.seda;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class SedaContext implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(SedaContext.class);

    private final Map<String, Stage> eventMapper = new ConcurrentHashMap<>();
    private final AtomicReference<ContextState> state = new AtomicReference<>(ContextState.RUNNING);


    public synchronized <T> void addStage(String name, Stage.Builder<T> builder) {
        if (eventMapper.containsKey(name)) {
            throw new IllegalArgumentException("Duplicated stage name '" + name + "'");
        }
        Stage<T> stage = builder.build(name, this);
        eventMapper.put(name, stage);
    }

    public Map<String, StageStats> stats() {
        return eventMapper.values().stream().collect(Collectors.toMap(Stage::name, Stage::stats));
    }

    @SuppressWarnings("unchecked")
    public void submit(String stageName, Object event) {
        ContextState contextState = state.get();
        String uuid = UUID.randomUUID().toString();
        if (!ContextState.RUNNING.equals(contextState)) {
            throw new IllegalStateException("Cannot accept new events, context state " + contextState);
        }
        Objects.requireNonNull(event, "Event must be provided");
        sendTo(stageName, uuid, event);
    }

    public ContextState state() {
        return state.get();
    }


    void sendToNextStage(String stageName, String correlationId, Object event) {
        ContextState contextState = state.get();
        if (ContextState.CLOSED.equals(contextState) || ContextState.CLOSING.equals(contextState)) {
            throw new IllegalStateException("Cannot accept new events, context state " + contextState);
        }
        Objects.requireNonNull(event, "Event must be provided");
       sendTo(stageName, correlationId, event);
    }

    @SuppressWarnings("unchecked")
    private void sendTo(String stageName, String correlationId, Object event) {
        Stage stage = eventMapper.get(stageName);
        if(stage == null) {
            throw new IllegalArgumentException("No such stage: " + stageName);
        }
        stage.submit(correlationId, event);
    }

    @Override
    public synchronized void close() {
        if (!state.compareAndSet(ContextState.RUNNING, ContextState.CLOSING)) {
            return;
        }
        closeInternal();
    }

    public synchronized void close(long timeout, TimeUnit unit) {
        if (!state.compareAndSet(ContextState.RUNNING, ContextState.CLOSING)) {
            return;
        }
        logger.info("Closing SEDA context");
        for (Stage stage : eventMapper.values()) {
            stage.close(timeout, unit);
        }
        state.set(ContextState.CLOSED);
    }

    private void closeInternal() {
        logger.info("Closing SEDA context");
        eventMapper.values().forEach(Stage::close);
        state.set(ContextState.CLOSED);
    }

    //Close the context and process remaining items in the stages
    public synchronized void shutdown() {
        if (!state.compareAndSet(ContextState.RUNNING, ContextState.AWAITING_COMPLETION)) {
            return;
        }

        boolean completed;
        try {
            do {
                logger.info("Waiting for stages to complete");

                completed = true;
                for (Stage stage : eventMapper.values()) {
                    StageStats stats = stage.stats();
                    boolean stageCompleted = stats.activeCount == 0 && stats.queueSize == 0;
                    completed = completed && stageCompleted;
                }

                sleep();
            } while (!completed);
            logger.info("All stages completed");

        } finally {
            closeInternal();
        }
    }

    private void sleep() {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
