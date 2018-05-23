package io.joshworks.fstore.core.seda;

import java.util.function.Consumer;

@FunctionalInterface
public interface StageHandler<T> extends Consumer<EventContext<T>> {

    @Override
    default void accept(final EventContext<T> elem) {
        try {
            acceptThrows(elem);
        } catch (Exception e) {
            throw new StageHandlerException(e);
        }
    }

    void acceptThrows(EventContext<T> elem) throws Exception;

    class StageHandlerException extends RuntimeException {
        private StageHandlerException(Throwable cause) {
            super(cause);
        }
    }

}