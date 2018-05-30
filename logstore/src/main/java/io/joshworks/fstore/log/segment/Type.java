package io.joshworks.fstore.log.segment;

public enum Type {
    EMPTY(0),
    LOG_HEAD(1),
    MERGE_OUT(2),
    READ_ONLY(3);

    final int val;

    Type(int i) {
        this.val = i;
    }

    static Type of(int type) {
        for (Type theType : Type.values()) {
            if (theType.val == type) {
                return theType;
            }
        }
        throw new IllegalArgumentException("Invalid type: " + type);

    }

}
