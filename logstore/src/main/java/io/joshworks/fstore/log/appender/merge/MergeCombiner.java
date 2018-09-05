package io.joshworks.fstore.log.appender.merge;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.segment.Log;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class MergeCombiner<T extends Comparable<T>>  implements SegmentCombiner<T> {

    protected abstract IteratorContainer<T> pollFirst();
    protected abstract void add(IteratorContainer<T> item);
    protected abstract boolean isEmpty();

    @Override
    public void merge(List<? extends Log<T>> segments, Log<T> output) {

        List<LogIterator<T>> iterators = segments.stream().map(s -> s.iterator(Direction.FORWARD)).collect(Collectors.toList());

        for (LogIterator<T> iterator : iterators) {
            IteratorContainer<T> container = new IteratorContainer<>(iterator);
            if (container.next()) {
                add(container);
            } else {
                IOUtils.closeQuietly(container);
            }
        }

        while (!isEmpty()) {
            IteratorContainer<T> ac = pollFirst();
            output.append(ac.current);

            if (ac.next()) {
                add(ac);
            }
        }

        for (LogIterator<T> iterator : iterators) {
            IOUtils.closeQuietly(iterator);
        }
    }

    static class IteratorContainer<T extends Comparable<T>> implements Comparable<IteratorContainer<T>>, Closeable {
        LogIterator<T> it;
        T current;

        public IteratorContainer(LogIterator<T> it) {
            this.it = it;
        }

        public boolean next() {
            current = it.hasNext() ? it.next() : null;
            return current != null;
        }

        @Override
        public int compareTo(IteratorContainer<T> o) {
            Objects.requireNonNull(current, "Current item is null");
            Objects.requireNonNull(o.current, "Other item is null");

            return current.compareTo(o.current);
        }

        @Override
        public void close() throws IOException {
            it.close();
        }
    }
}
