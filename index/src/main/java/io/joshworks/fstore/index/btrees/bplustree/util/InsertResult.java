package io.joshworks.fstore.index.btrees.bplustree.util;

public class InsertResult<V> extends Result<V> {



    public InsertResult previousValue(final V previousValue) {
        super.foundValue = previousValue;
        return this;
    }

    @Override
    public InsertResult newRootId(int newRootId) {
        super.newRootId(newRootId);
        return this;
    }
}
