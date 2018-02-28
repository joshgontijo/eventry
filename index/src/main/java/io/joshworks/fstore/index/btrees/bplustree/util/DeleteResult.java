package io.joshworks.fstore.index.btrees.bplustree.util;

public class DeleteResult<V> extends Result  {

    public final V deleted;

    private DeleteResult(int newRootId, V deleted) {
        super(newRootId);
        this.deleted = deleted;
    }

    public static <V> DeleteResult<V> of(int newRootId, V deleted) {
        return new DeleteResult<>(newRootId, deleted);
    }

    public static <V> DeleteResult<V> of(V deleted) {
        return new DeleteResult<>(NO_NEW_ROOT, deleted);
    }

    public static <V> DeleteResult<V> notDeleted() {
        return new DeleteResult<>(NO_NEW_ROOT, null);
    }
}
