package io.joshworks.fstore.index.bplustree.util;

public class InsertResult<V> extends Result<V> {


    public InsertResult<V> previousValue(final V previousValue) {
        super.foundValue = previousValue;
        return this;
    }

    @Override
    public InsertResult<V> newRootId(int newRootId) {
        super.newRootId(newRootId);
        return this;
    }
}
