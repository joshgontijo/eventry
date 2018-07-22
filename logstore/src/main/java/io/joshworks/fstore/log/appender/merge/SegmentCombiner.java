package io.joshworks.fstore.log.appender.merge;

import io.joshworks.fstore.log.segment.Log;

import java.util.List;

public interface SegmentCombiner<T> {

    void merge(List<? extends Log<T>> segments, Log<T> output);

}
