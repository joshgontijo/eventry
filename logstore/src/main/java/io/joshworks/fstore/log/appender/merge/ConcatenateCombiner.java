package io.joshworks.fstore.log.appender.merge;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class ConcatenateCombiner<T> implements SegmentCombiner<T> {

    @Override
    public void merge(List<Stream<T>> segments, Consumer<T> writer) {
        segments.stream().flatMap(l -> l).forEach(writer::accept);
    }
}
