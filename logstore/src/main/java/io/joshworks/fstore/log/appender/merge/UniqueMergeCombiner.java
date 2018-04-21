package io.joshworks.fstore.log.appender.merge;

import java.util.TreeSet;

public class UniqueMergeCombiner<T extends Comparable<T>> extends MergeCombiner<T> {

    private final TreeSet<IteratorContainer<T>> set = new TreeSet<>();

    @Override
    protected IteratorContainer<T> pollFirst() {
        return set.pollFirst();
    }

    @Override
    protected void add(IteratorContainer<T> item) {
        set.add(item);
    }

    @Override
    protected boolean isEmpty() {
        return set.isEmpty();
    }
}
