package io.joshworks.logstore;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class Index {

    private static final Map<String, Entry> byUuid = new ConcurrentHashMap<>();
    private static final Map<String, SortedSet<Entry>> byStream = new ConcurrentHashMap<>();



    public long index(String uuid, String stream, Entry entry) {
        indexUuid(uuid, entry);
        return indexStream(stream, entry);
    }

    public void indexUuid(String uuid, Entry entry) {
        byUuid.put(uuid, entry);
    }

    //Not thread safe
    //returning the stream version just to demonstrate the idea
    public long indexStream(String stream, Entry entry) {
        byStream.putIfAbsent(stream, new TreeSet<>(Comparator.comparingLong(a -> a.offset)));
        boolean newValue = byStream.get(stream).add(entry);
        if(!newValue) {
            System.err.println("[WARNING] Duplicated index key found on stream: " + stream);
        }
        return byStream.get(stream).size();
    }

    public Entry get(String uuid) {
        return byUuid.getOrDefault(uuid, Entry.NONE);
    }

    public Set<Entry> getStream(String stream) {
        return byStream.getOrDefault(stream, new TreeSet<>());
    }

    /**
     * Returns the stream version for a given event in a stream
     * @param stream The stream
     * @param uuid The event UUID
     * @return -1 if the event doesn't exist, 0 if the event is not on this stream or the position on the stream, starting with 1
     */
    public long streamVersion(String stream, String uuid) {
        final Entry found = byUuid.getOrDefault(uuid, Entry.NONE);
        if(Entry.NONE.equals(found)) {
            return -1;
        }
        SortedSet<Entry> positions = byStream.getOrDefault(stream, new TreeSet<>());
        return positions.stream().mapToLong(e -> e.offset).filter(p -> p <= found.offset).count();
    }

    public void clear() {
        byUuid.clear();
        byStream.clear();
    }

    public static class Entry  {
        public final long offset;
        public final int length;

        public static Entry NONE = Entry.of(-1, 0);

        private Entry(long offset, int length) {
            this.offset = offset;
            this.length = length;
        }

        public static Entry of(long offset, int length) {
            return new Entry(offset, length);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Entry entry = (Entry) o;

            if (offset != entry.offset) return false;
            return length == entry.length;
        }

        @Override
        public int hashCode() {
            int result = (int) (offset ^ (offset >>> 32));
            result = 31 * result + length;
            return result;
        }
    }

}
