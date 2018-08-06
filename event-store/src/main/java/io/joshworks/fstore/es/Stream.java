package io.joshworks.fstore.es;

import java.util.HashMap;
import java.util.Map;

public class Stream {

    public final String name;
    public final long hash;
    public final long created;

    private long maxAge = -1;
    private int maxCount = -1;
    private final Map<String, Permission> permissions = new HashMap<>() ;
    private final Map<String, String> data = new HashMap<>();

    public Stream(String name, long hash, long created) {
        this.name = name;
        this.hash = hash;
        this.created = created;
    }

    public boolean hasReadPermission(String id) {
        return permissions.getOrDefault(id, Permission.NONE).equals(Permission.READ);
    }

    public boolean hasWritePermission(String id) {
        return permissions.getOrDefault(id, Permission.NONE).equals(Permission.WRITE);
    }

    public String metadata(String key) {
        return data.get(key);
    }


    public enum Permission {
        NONE, READ, WRITE
    }

}

