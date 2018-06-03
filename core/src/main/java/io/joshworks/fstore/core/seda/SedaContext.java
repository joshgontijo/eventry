package io.joshworks.fstore.core.seda;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class SedaContext implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(SedaContext.class);

    private final Map<Class, List<Stage>> eventMapper = new ConcurrentHashMap<>();
    private final AtomicReference<ContextState> state = new AtomicReference<>(ContextState.RUNNING);


    public synchronized <T> void addStage(Class<T> eventType, Stage.Builder<T> builder) {
        Stage<T> stage = builder.build(this);
        boolean containsName = eventMapper.values().stream().flatMap(Collection::stream).anyMatch(s -> s.name().equals(stage.name()));
        if (containsName) {
            throw new IllegalArgumentException("Duplicated stage name '" + stage.name() + "'");
        }


        eventMapper.putIfAbsent(eventType, new ArrayList<>());
        eventMapper.get(eventType).add(stage);
    }

    public Map<String, StageStats> stats() {
        return eventMapper.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(Stage::name, Stage::stats));
    }

    public void submit(Object event) {
        submit(null, event);
    }

    @SuppressWarnings("unchecked")
    public void submit(String correlationId, Object event) {
        ContextState contextState = state.get();
        if (!ContextState.RUNNING.equals(contextState)) {
            throw new IllegalStateException("Cannot accept new events, context state " + contextState);
        }
        Objects.requireNonNull(event, "Event must be provided");
        List<Stage> stages = eventMapper.getOrDefault(event.getClass(), new ArrayList<>());
        //TODO overal load ? how to tell the previous stage about the load of the next
        for (Stage stage : stages) {
            stage.submit(correlationId, event);
        }
    }

    public ContextState state() {
        return state.get();
    }

    @SuppressWarnings("unchecked")
    void sendToNextStage(String correlationId, Object event) {
        ContextState contextState = state.get();
        if (ContextState.CLOSED.equals(contextState) || ContextState.CLOSING.equals(contextState)) {
            throw new IllegalStateException("Cannot accept new events, context state " + contextState);
        }
        Objects.requireNonNull(event, "Event must be provided");
        List<Stage> stages = eventMapper.getOrDefault(event.getClass(), new ArrayList<>());
        //TODO overal load ? how to tell the previous stage about the load of the next
        for (Stage stage : stages) {
            stage.submit(correlationId, event);
        }
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
        List<Stage> stages = eventMapper.values().stream().flatMap(List::stream).collect(Collectors.toList());
        for (Stage stage : stages) {
            stage.close(timeout, unit);
        }
        state.set(ContextState.CLOSED);
    }

    private void closeInternal() {
        logger.info("Closing SEDA context");
        eventMapper.values().stream().flatMap(List::stream).forEach(Stage::close);
        state.set(ContextState.CLOSED);
    }

    //Close the context and process remaining items in the stages
    public synchronized void shutdown() {
        if (!state.compareAndSet(ContextState.RUNNING, ContextState.AWAITING_COMPLETION)) {
            return;
        }
        List<Stage> stages = eventMapper.values().stream().flatMap(List::stream).collect(Collectors.toList());

        boolean completed;

        try {
            do {
                logger.info("Waiting for stages to complete");

                completed = true;
                for (Stage stage : stages) {
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
