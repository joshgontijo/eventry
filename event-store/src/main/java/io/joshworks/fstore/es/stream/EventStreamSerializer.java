package io.joshworks.fstore.es.stream;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.serializer.VStringSerializer;
import io.joshworks.fstore.serializer.collection.MapSerializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EventStreamSerializer implements Serializer<EventStream> {

    private final Serializer<Map<String, Integer>> permissionSerializer = Serializers.mapSerializer(Serializers.VSTRING, Serializers.INTEGER, VStringSerializer::sizeOf, v -> Integer.BYTES, ConcurrentHashMap::new);
    private final Serializer<Map<String, String>> metadataSerializer = Serializers.mapSerializer(Serializers.VSTRING, Serializers.VSTRING, VStringSerializer::sizeOf, VStringSerializer::sizeOf, ConcurrentHashMap::new);

    @Override
    public ByteBuffer toBytes(EventStream data) {
        int nameSize = VStringSerializer.sizeOf(data.name);
        int permissionsSize = MapSerializer.sizeOfMap(data.permissions, VStringSerializer::sizeOf, v -> Integer.BYTES);
        int metadataSize = MapSerializer.sizeOfMap(data.metadata, VStringSerializer::sizeOf, VStringSerializer::sizeOf);
        int permissionsMapLength = Integer.BYTES;
        int metadataMapLength = Integer.BYTES;
        ByteBuffer bb = ByteBuffer.allocate(nameSize + (Long.BYTES * 3) + Integer.BYTES + permissionsSize + metadataSize + permissionsMapLength + metadataMapLength);

        writeTo(data, bb);
        return bb.flip();

    }

    @Override
    public void writeTo(EventStream data, ByteBuffer dest) {
        dest.putLong(data.hash);
        dest.putLong(data.created);
        dest.putLong(data.maxAge);
        dest.putInt(data.maxCount);
        dest.put(Serializers.VSTRING.toBytes(data.name));

        permissionSerializer.writeTo(data.permissions, dest);
        metadataSerializer.writeTo(data.metadata, dest);
    }

    @Override
    public EventStream fromBytes(ByteBuffer buffer) {
        long hash = buffer.getLong();
        long created = buffer.getLong();
        long maxAge = buffer.getLong();
        int maxCount = buffer.getInt();
        String name = Serializers.VSTRING.fromBytes(buffer);

        Map<String, Integer> permissions = permissionSerializer.fromBytes(buffer);
        Map<String, String> metadata = metadataSerializer.fromBytes(buffer);

        return new EventStream(name, hash, created, maxAge, maxCount, permissions, metadata);
    }

}
