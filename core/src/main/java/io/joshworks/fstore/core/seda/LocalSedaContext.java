package io.joshworks.fstore.core.seda;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class LocalSedaContext implements SedaContext {

    private static final Logger logger = LoggerFactory.getLogger(LocalSedaContext.class);

    private final Map<Class, List<Stage>> eventMapper = new ConcurrentHashMap<>();


    @Override
    public <T> void addStage(Class<T> eventType, Stage.Builder<T> builder) {
        Stage<T> stage = builder.build(this);

        eventMapper.putIfAbsent(eventType, new ArrayList<>());
        eventMapper.get(eventType).add(stage);
    }

    @Override
    public void submit(Object event) {
        Objects.requireNonNull(event, "Event must be provided");
        List<Stage> stages = eventMapper.get(event.getClass());
        for (Stage stage : stages) {
            stage.submit(null, event);
        }
    }

    @Override
    public void submit(String correlationId, Object event) {
        Objects.requireNonNull(event, "Event must be provided");
        List<Stage> stages = eventMapper.get(event.getClass());
            //TODO overal load ? how to tell the previous stage about the load of the next
        for (Stage stage : stages) {
            stage.submit(correlationId, event);
        }
    }

    @Override
    public void close() {
        logger.info("Closing SEDA context");
        eventMapper.values().stream().flatMap(List::stream).forEach(Stage::close);
        eventMapper.clear();
    }
}
