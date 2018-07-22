package io.joshworks.fstore.log.appender.merge;

import io.joshworks.fstore.log.segment.Log;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class MergeCombiner<T extends Comparable<T>>  implements SegmentCombiner<T> {

    protected abstract IteratorContainer<T> pollFirst();
    protected abstract void add(IteratorContainer<T> item);
    protected abstract boolean isEmpty();

    @Override
    public void merge(List<? extends Log<T>> segments, Log<T> output) {

        List<Iterator<T>> iterators = segments.stream().map(Log::iterator).collect(Collectors.toList());

        for (Iterator<T> iterator : iterators) {
            IteratorContainer<T> container = new IteratorContainer<>(iterator);
            if (container.next()) {
                add(container);
            }
        }

        while (!isEmpty()) {
            IteratorContainer<T> ac = pollFirst();
            output.append(ac.current);

            if (ac.next()) {
                add(ac);
            }
        }
    }

    static class IteratorContainer<T extends Comparable<T>> implements Comparable<IteratorContainer<T>> {
        Iterator<T> it;
        T current;

        public IteratorContainer(Iterator<T> it) {
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
    }
}
