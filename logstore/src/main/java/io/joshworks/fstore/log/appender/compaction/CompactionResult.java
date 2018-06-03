package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.log.segment.Log;

import java.util.List;

class CompactionResult<T, L extends Log<T>> {

    final Exception exception;
    final List<L> sources;
    final L target; //nullable
    final int level;

    private CompactionResult(List<L> segments, L target, int level, Exception exception) {
        this.exception = exception;
        this.sources = segments;
        this.target = target;
        this.level = level;
    }

    static <T, L extends Log<T>> CompactionResult<T, L> success(List<L> segments, L target, int level) {
        return new CompactionResult<>(segments, target, level, null);
    }

    static <T, L extends Log<T>> CompactionResult<T, L> failure(List<L> segments, L target, int level, Exception exception) {
        return new CompactionResult<>(segments, target, level, exception);
    }

    boolean successful() {
        return exception == null;
    }
}
