package io.joshworks.fstore.core.seda;

public interface Publisher {

    void submit(Object event);

    void submit(String correlationId, Object event);
}
