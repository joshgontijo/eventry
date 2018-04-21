package io.joshworks.fstore.log.appender.merge;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

@FunctionalInterface
public interface SegmentCombiner<T> {

    void merge(List<Stream<T>> segments, Consumer<T> writer);

}
