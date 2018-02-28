package io.joshworks.fstore.index.btrees.bplustree.util;

public class InsertResult extends Result {

    public final boolean inserted; //TODO add if was duplicated

    private InsertResult(int newRootId, boolean inserted) {
        super(newRootId);
        this.inserted = inserted;
    }

    public static InsertResult of(int newRootId, boolean inserted) {
        return new InsertResult(newRootId, inserted);
    }

    public static InsertResult noSplit() {
        return new InsertResult(Result.NO_NEW_ROOT, true);
    }
}
