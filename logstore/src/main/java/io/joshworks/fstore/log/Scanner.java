package io.joshworks.fstore.log;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class Scanner<T> implements Iterator<T> {

    protected final Storage storage;
    protected final DataReader reader;
    protected final Serializer<T> serializer;

    private T data;
    protected boolean completed = false;
    protected long position;

    public Scanner(Storage storage, DataReader reader, Serializer<T> serializer, long position) {
        this.storage = storage;
        this.reader = reader;
        this.serializer = serializer;
        this.position = position;
    }

    protected abstract T readAndVerify();

    @Override
    public boolean hasNext() {
        if (completed) {
            return false;
        }
        this.data = readAndVerify();
        this.completed = this.data == null;
        return !completed;
    }

    @Override
    public T next() {
        if (completed) {
            throw new NoSuchElementException();
        }
        if (data == null) {
            this.data = readAndVerify();
            if (data == null) {
                completed = true;
                throw new NoSuchElementException();
            }
        }
        T temp = data;
        data = null;
        return temp;
    }

    public long position() {
        return position;
    }

}
