package io.joshworks.fstore.core.seda;

@FunctionalInterface
public interface StageHandler<T>  {

    default void handle(final EventContext<T> elem) {
        try {
            onEvent(elem);
        } catch (Exception e) {
            throw new StageHandlerException(e);
        }
    }

    void onEvent(EventContext<T> elem) throws Exception;

    class StageHandlerException extends RuntimeException {
        private StageHandlerException(Throwable cause) {
            super(cause);
        }
    }

}