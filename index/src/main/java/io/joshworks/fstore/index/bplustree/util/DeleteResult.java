package io.joshworks.fstore.index.bplustree.util;

public class DeleteResult<V> extends Result<V>  {


    public DeleteResult deleted(final V previousValue) {
        this.foundValue = previousValue;
        return this;
    }



}
