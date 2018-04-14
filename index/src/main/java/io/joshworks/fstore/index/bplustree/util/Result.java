package io.joshworks.fstore.index.bplustree.util;

public class Result<V> {

    public static final int NO_NEW_ROOT = -1;
    public int newRootId = NO_NEW_ROOT;

    protected V foundValue;

    public Result newRootId(final int newRootId) {
        this.newRootId = newRootId;
        return this;
    }

    public V foundValue() {
        return foundValue;
    }
}
