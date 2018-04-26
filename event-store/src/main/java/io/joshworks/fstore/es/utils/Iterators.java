package io.joshworks.fstore.es.utils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

public class Iterators<T> {

    private Iterators() {

    }

    public static <T> Iterator<T> reversed(List<T> original) {
        return new ReversedIterator<>(original);
    }

    public static <T> Iterator<T> concat(List<Iterator<T>> original) {
        return new IteratorIterator<>(original);
    }

    public static <T> Iterator<T> concat(Iterator<T>... originals) {
        return new IteratorIterator<>(Arrays.asList(originals));
    }

    public static <T> Iterator<T> empty() {
        return new EmptyIterator<>();
    }

    private static class EmptyIterator<T> implements Iterator<T> {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            throw new NoSuchElementException();
        }
    }

    private static class ReversedIterator<T> implements Iterator<T> {
        final ListIterator<T> i;

        private ReversedIterator(List<T> original) {
            this.i = original.listIterator(original.size());
        }

        public boolean hasNext() {
            return i.hasPrevious();
        }

        public T next() {
            return i.previous();
        }

        public void remove() {
            i.remove();
        }
    }

    private static class IteratorIterator<T> implements Iterator<T> {

        private final List<Iterator<T>> is;
        private int current;

        private IteratorIterator(List<Iterator<T>> iterators) {
            this.is = iterators;
            this.current = 0;
        }

        @Override
        public boolean hasNext() {
            while (current < is.size() && !is.get(current).hasNext())
                current++;

            return current < is.size();
        }

        @Override
        public T next() {
            while (current < is.size() && !is.get(current).hasNext())
                current++;

            return is.get(current).next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}