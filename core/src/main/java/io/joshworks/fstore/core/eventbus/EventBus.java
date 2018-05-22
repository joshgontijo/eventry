package io.joshworks.fstore.core.eventbus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;


/**
 * Created by Josue on 05/08/2016.
 */
public class EventBus {

    private final Map<Class, List<Subscriber>> subscribers = new ConcurrentHashMap<>();
    private final ExecutorService executor;
    private final ErrorHandler errorHandler;

    private static final Logger logger = LoggerFactory.getLogger(EventBus.class);
    private static final ErrorHandler DEFAULT_ERROR_HANDLER =
            (e, ctx) -> logger.error("Error on " + ctx.handler.getClass().getSimpleName() + "#" + ctx.method, e);

    public EventBus() {
        this(Executors.newCachedThreadPool(), DEFAULT_ERROR_HANDLER);
    }

    public EventBus(ExecutorService executor) {
        this(executor, DEFAULT_ERROR_HANDLER);
    }

    public EventBus(ErrorHandler errorHandler) {
        this(Executors.newCachedThreadPool(), errorHandler);
    }

    public EventBus(ExecutorService executor, ErrorHandler errorHandler) {
        this.executor = executor;
        this.errorHandler = errorHandler;
    }

    public void emitAsync(final Object event) {
        for (Subscriber subscriber : getSubscribers(event)) {
            executor.submit(() -> subscriber.invoke(event, errorHandler));
        }
    }

    public void emit(final Object event) {
        for (Subscriber subscriber : getSubscribers(event)) {
            subscriber.invoke(event, errorHandler);
        }
    }

    private List<Subscriber> getSubscribers(Object event) {
        Objects.requireNonNull(event, "Event must be provided");
        List<Subscriber> found = subscribers.get(event.getClass());
        if (found == null) {
            logger.warn("No handler for type found for type {}", event.getClass());
            return new ArrayList<>();
        }
        return found;
    }

    public void register(Object subscriber) {
        Objects.requireNonNull(subscriber, "Subscriber cannot be null");

        for (Method method : subscriber.getClass().getDeclaredMethods()) {
            if (method.isAnnotationPresent(Subscribe.class)) {
                Parameter[] parameters = method.getParameters();
                if (parameters.length != 1) {
                    throw new IllegalArgumentException("Subscriber must accept only 1 parameter");
                }
                Class<?> parameterType = parameters[0].getType();
                subscribers.putIfAbsent(parameterType, new ArrayList<>());

                boolean synchronize = method.getAnnotation(Subscribe.class).synchronize();

                logger.info(":: Added Subscriber {}#{}[{}] ::", subscriber.getClass().getSimpleName(), method.getName(), parameterType.getSimpleName());
                subscribers.get(parameterType).add(new ObjectSubscriber(subscriber, method, synchronize));
            }
        }
    }

    public <T> void on(Class<T> eventType, EventHandler<T> handler) {
        Objects.requireNonNull(eventType, "Event type must be provided");
        Objects.requireNonNull(handler, "Handler must be provided");

        subscribers.putIfAbsent(eventType, new ArrayList<>());
        subscribers.get(eventType).add(new FunctionSubscriber(handler));
    }

    private interface Subscriber {
        void invoke(Object event, ErrorHandler errorHandler);
    }

    private static class ObjectSubscriber implements Subscriber {
        final Object object;
        final Method method;
        final boolean synchronize;

        private ObjectSubscriber(Object object, Method method, boolean synchronize) {
            this.object = object;
            this.method = method;
            this.synchronize = synchronize;
        }

        @Override
        public void invoke(Object event, ErrorHandler errorHandler) {
            try {
                if (synchronize) {
                    synchronized (object) {
                        method.invoke(object, event);
                    }
                } else {
                    method.invoke(object, event);
                }
            } catch (IllegalAccessException e) {
                logger.error("Cannot access " + object + "#" + method, e);
            } catch (IllegalArgumentException e) {
                logger.error("Invalid argument for " + object + "#" + method, e);
            } catch (InvocationTargetException e) {
                errorHandler.accept(e.getCause(), new Context(method.getName(), event, object));
            }
        }
    }

    private static class FunctionSubscriber implements Subscriber {
        final EventHandler handler;

        private FunctionSubscriber(EventHandler handler) {
            this.handler = handler;
        }

        @Override
        public void invoke(Object event, ErrorHandler errorHandler) {
            try {
                handler.accept(event);
            } catch (FunctionException e) {
                errorHandler.accept(e.getCause(), new Context(handler.toString(), event, null));
            } catch (Exception e) {
                logger.error("Invalid argument for " + handler.toString(), e);
            }
        }
    }

    @FunctionalInterface
    public interface EventHandler<T> extends Consumer<T> {

        @Override
        default void accept(final T elem) {
            try {
                acceptThrows(elem);
            } catch (Exception e) {
                throw new FunctionException(e);
            }
        }

        void acceptThrows(T elem) throws Exception;
    }

    private static class FunctionException extends RuntimeException {
        private FunctionException(Throwable cause) {
            super(cause);
        }
    }


}
