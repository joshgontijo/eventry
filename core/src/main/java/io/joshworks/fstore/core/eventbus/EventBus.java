package io.joshworks.fstore.core.eventbus;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Josue on 05/08/2016.
 */
public class EventBus {

    private static final Logger logger = Logger.getLogger(EventBus.class.getName());

    private static final Map<Class, List<Subscriber>> subscribers = new ConcurrentHashMap<>();
    private static final ExecutorService executor = Executors.newCachedThreadPool();

    private EventBus() {
    }

    public static void fire(final Object event) {
        Objects.requireNonNull(event, "Event must be provided");
        List<Subscriber> subscribers = EventBus.subscribers.get(event.getClass());
        if (subscribers == null) {
            logger.warning("No handler for type found for type " + event.getClass());
            return;
        }
        for (Subscriber listener : subscribers) {
            if (listener.async) {
                executor.submit(() -> invoke(listener, event));
            } else {
                invoke(listener, event);
            }
        }
    }

    private static Object invoke(Subscriber listener, Object event) {
        try {
            return listener.method.invoke(listener.object, event);
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            return null;
        }
    }

    public static void register(Object subscriber) {
        Objects.requireNonNull(subscriber, "Subscriber cannot be null");

        for (Method method : subscriber.getClass().getDeclaredMethods()) {
            if (method.isAnnotationPresent(Subscribe.class)) {
                Parameter[] parameters = method.getParameters();
                if (parameters.length != 1) {
                    throw new IllegalArgumentException("Subscriber must accept only 1 parameter");
                }
                Class<?> parameterType = parameters[0].getType();
                if (!subscribers.containsKey(parameterType)) {
                    subscribers.put(parameterType, new LinkedList<>());
                }

                boolean async = method.getAnnotation(Subscribe.class).async();

                logger.log(Level.INFO, ":: Added Subscriber {0}#{1}[{2}] ::",
                        new Object[]{subscriber.getClass().getSimpleName(), method.getName(), parameterType.getSimpleName()});
                subscribers.get(parameterType).add(new Subscriber(subscriber, method, async));
            }
        }
    }

    private static class Subscriber {
        final Object object;
        final Method method;
        final boolean async;

        private Subscriber(Object object, Method method, boolean async) {
            this.object = object;
            this.method = method;
            this.async = async;
        }
    }
}
