package io.joshworks.fstore.index.btrees.bplustree.util;

public class Result {

    public static final int NO_NEW_ROOT = -1;
    public final int newRootId;

    Result(int newRootId) {
        this.newRootId = newRootId;
    }
}
