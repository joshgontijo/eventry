package io.joshworks.logstore;

import io.joshworks.fstore.api.Event;
import io.joshworks.fstore.api.EventStream;
import io.joshworks.fstore.api.Serializer;
import io.joshworks.fstore.utils.Identifiers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.function.Consumer;

public class EventLogStore {

    private final LogStore store;
    private final Serializer serializer;
    private final LogScanner scanner;

    static final int INT_SIZE = 4;

    private final Index index = new Index();

    public EventLogStore(LogStore store, LogScanner scanner, Serializer serializer) {
        this.store = store;
        this.serializer = serializer;
        this.scanner = scanner;
        buildIndex();
    }

    public Event save(String stream, Event event) {
        try {
            event.setUuid(Identifiers.shortUuid());

            byte[] data = serializer.toBytes(event);
            int eventDataLength = data.length;
            data = appendLength(data);

            long position = store.append(ByteBuffer.wrap(data));
            position = position + INT_SIZE; //length field offset

            index.index(event.getUuid(), stream, Index.Entry.of(position, eventDataLength));

            return event;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Event get(String uuid) {
        Index.Entry found = index.get(uuid);
        if (Index.Entry.NONE.equals(found)) {
            throw new RuntimeException("Event not found for uuid: " + uuid);
        }
        return read((int) found.offset, found.length);
    }

    public EventStream getStream(String stream) {
        Set<Index.Entry> found = index.getStream(stream);

        EventStream eventStream = EventStream.empty();
        for (Index.Entry entry : found) {
            eventStream.add(read((int) entry.offset, entry.length));
        }

        return eventStream;
    }

    public void forEach(Consumer<Event> consumer) {
        scanner.forEach(e -> {
            Event event = serializer.fromBytes(e.data.array());
            consumer.accept(event);
        });
    }

    private void buildIndex() {
        System.out.println("Rebuilding PK index");
        scanner.forEach(e -> {
            Event event = serializer.fromBytes(e.data.array());
            index.indexUuid(event.getUuid(), Index.Entry.of(e.position, e.data.remaining()));
        });
    }

    private Event read(int offset, int length) {
        byte[] data = new byte[0];
        try {
            data = new byte[length];
            store.read(ByteBuffer.wrap(data), offset);
            return serializer.fromBytes(data);

        } catch (Exception e) {
            System.err.println("ERROR: |" + new String(data) + "|");
            throw new RuntimeException(e);
        }
    }

    /**
     * Writes the event in the store format:
     * length data lineBreak
     */
    private byte[] appendLength(byte[] data) {
        ByteBuffer buffer = ByteBuffer.allocate(INT_SIZE + data.length);
        buffer.putInt(data.length).put(data);
        return buffer.array();
    }

    public void close() {
        try {
            store.close();
            scanner.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
