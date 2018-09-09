package io.joshworks.eventry.stream;

import java.util.Map;

public class StreamInfo {

    public final String name;
    public final long hash;
    public final long created;

    public final long maxAge;
    public final int maxCount;
    public final int version;
    final Map<String, Integer> permissions;
    final Map<String, String> metadata;

    public StreamInfo(String name, long hash, long created, long maxAge, int maxCount, int version, Map<String, Integer> permissions, Map<String, String> metadata) {
        this.name = name;
        this.hash = hash;
        this.created = created;
        this.maxAge = maxAge;
        this.maxCount = maxCount;
        this.version = version;
        this.permissions = permissions;
        this.metadata = metadata;
    }

    public static StreamInfo from(StreamMetadata metadata, int version) {
        return new StreamInfo(metadata.name, metadata.hash, metadata.created, metadata.maxAge, metadata.maxCount, version, metadata.permissions, metadata.metadata);
    }

}

