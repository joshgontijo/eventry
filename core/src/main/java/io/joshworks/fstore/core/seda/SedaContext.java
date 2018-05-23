package io.joshworks.fstore.core.seda;

import java.io.Closeable;

public interface SedaContext extends Publisher, Closeable {

    <T> void addStage(Class<T> eventType, Stage.Builder<T> builder);

}
