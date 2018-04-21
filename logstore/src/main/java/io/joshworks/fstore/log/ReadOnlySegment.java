package io.joshworks.fstore.log;

import java.io.IOException;
import java.util.Objects;
import java.util.stream.Stream;

public class ReadOnlySegment<T> implements Log<T> {

    private final Log<T> delegate;

    public ReadOnlySegment(Log<T> delegate) {
        Objects.requireNonNull(delegate);
        this.delegate = delegate;
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public Scanner<T> scanner() {
        return delegate.scanner();
    }

    @Override
    public Stream<T> stream() {
        return delegate.stream();
    }

    @Override
    public Scanner<T> scanner(long position) {
        return delegate.scanner(position);
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public T get(long position) {
        return delegate.get(position);
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public long checkIntegrity(long lastKnownPosition) {
        return delegate.checkIntegrity(lastKnownPosition);
    }

    @Override
    public void delete() {
        delegate.delete(); //TODO do nothing here ?
    }

    @Override
    public void complete() {
        throw new ReadOnlyException();
    }

    @Override
    public long append(T data) {
        throw new ReadOnlyException();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void flush() {
        throw new ReadOnlyException();
    }

    public static class ReadOnlyException extends RuntimeException{}

}
