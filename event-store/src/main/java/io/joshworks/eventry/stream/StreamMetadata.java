package io.joshworks.eventry.stream;

import java.util.HashMap;
import java.util.Map;

public class StreamMetadata {

    public final String name;
    public final long hash;
    public final long created;

    public final long maxAge;
    public final int maxCount;

    public final int state;

    final Map<String, Integer> permissions;
    final Map<String, String> metadata;

    public StreamMetadata(String name, long hash, long created) {
        this.name = name;
        this.hash = hash;
        this.created = created;
        this.maxAge = -1;
        this.maxCount = -1;
        this.permissions = new HashMap<>();
        this.metadata = new HashMap<>();
    }

    public StreamMetadata(String name, long hash, long created, long maxAge, int maxCount, Map<String, Integer> permissions, Map<String, String> metadata) {
        this.name = name;
        this.hash = hash;
        this.created = created;
        this.maxAge = maxAge;
        this.maxCount = maxCount;
        this.permissions = permissions;
        this.metadata = metadata;
    }

    public boolean hasReadPermission(String id) {
        return permissions.getOrDefault(id, PERMISSION_NONE).equals(PERMISSION_READ);
    }

    public boolean hasWritePermission(String id) {
        return permissions.getOrDefault(id, PERMISSION_NONE).equals(PERMISSION_WRITE);
    }

    public boolean

    public String metadata(String key) {
        return metadata.get(key);
    }

    public String name() {
        return name;
    }


    //not using enums to easier serialization
    public static final int PERMISSION_NONE = 0;
    public static final int PERMISSION_READ = 1;
    public static final int PERMISSION_WRITE = 2;

    public static final int STATUS_ACTIVE = 0;
    public static final int STATUS_LOCKED = 1;
    public static final int STATUS_DELETED = 2;

}

