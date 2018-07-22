package io.joshworks.fstore.log.appender.merge;

import io.joshworks.fstore.log.segment.Log;

import java.util.List;

public class ConcatenateCombiner<T> implements SegmentCombiner<T> {

    @Override
    public void merge(List<? extends Log<T>> segments, Log<T> output) {
        segments.stream()
                .map(Log::stream)
                .flatMap(l -> l)
                .forEach(output::append);
    }
}
