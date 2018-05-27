package io.joshworks.fstore.core.seda;

public interface Publisher {

    void publish(Object event);

    void publish(String correlationId, Object event);
}
