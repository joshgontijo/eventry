package io.joshworks.fstore.core.eventbus;

import java.util.function.BiConsumer;

@FunctionalInterface
public interface ErrorHandler extends BiConsumer<Throwable, Context> {

}
