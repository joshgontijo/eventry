package io.joshworks.fstore.server;

import java.util.Map;

public class NewStream {

    public final String name;
    public final int maxCount;
    public final long maxAge;
    public final Map<String, Integer> permissions;
    public final Map<String, String> metadata;

    public NewStream(String name, int maxCount, long maxAge, Map<String, Integer> permissions, Map<String, String> metadata) {
        this.name = name;
        this.maxCount = maxCount;
        this.maxAge = maxAge;
        this.permissions = permissions;
        this.metadata = metadata;
    }
}
