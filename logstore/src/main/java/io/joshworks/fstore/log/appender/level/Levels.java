package io.joshworks.fstore.log.appender.level;

import io.joshworks.fstore.log.appender.Order;
import io.joshworks.fstore.log.segment.Log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class Levels<T, L extends Log<T>> {

    private final SortedMap<Integer, List<L>> items;
    private L current;
    private final int maxItemsPerLevel;

    private final Set<L> compacting = new HashSet<>();

    private Levels(int maxItemsPerLevel, List<L> segments) {
        this.maxItemsPerLevel = maxItemsPerLevel;

        Map<Integer, List<L>> mappedLevels = segments.stream().collect(Collectors.groupingBy(Log::level));
        mappedLevels.values().forEach(segs -> segs.sort(Comparator.comparingLong(Log::created)));
        this.items = new ConcurrentSkipListMap<>(mappedLevels);

        if (!mappedLevels.containsKey(0)) {
            throw new IllegalStateException("Level zero must be present");
        }
        this.current = mappedLevels.get(0).get(0);
    }


    public synchronized List<L> segments(int level) {
        return items.get(level);
    }

    public L get(int segmentIdx) {
        //TODO improve performance
        return items.values().stream().flatMap(Collection::stream).collect(Collectors.toList()).get(segmentIdx);
    }

    public int depth() {
        return items.size();
    }

    public static <T, L extends Log<T>> Levels<T, L> load(int maxItemsPerLevel, List<L> segments) {
        return new Levels<>(maxItemsPerLevel, segments);
    }

    public void promoteLevelZero(L newLevelZero) {
        List<L> levelZero = items.get(0);
        if (levelZero != null) {
            items.putIfAbsent(1, new ArrayList<>(maxItemsPerLevel));
            L toBePromoted = levelZero.remove(0);
            toBePromoted.roll(1);

            if (!toBePromoted.readOnly()) {
                throw new IllegalStateException("Segment must be marked as read only after rolling");
            }

            items.get(1).add(toBePromoted);
        }
        items.get(0).add(newLevelZero);
        this.current = newLevelZero;
    }

//    private LinkedList<L> flatten() {
//        return new ArrayList<>(items).stream().flatMap(Collection::stream).collect(Collectors.toCollection(LinkedList::new));
//    }

    public int numSegments() {
        return (int) items.values().stream().mapToLong(Collection::size).sum();
    }

    public synchronized int size(int level) {
        if (level < 0) {
            throw new IllegalArgumentException("Level must be at least zero");
        }
        if (level >= depth()) {
            return 0;
        }
        return segments(level).size();
    }

    public synchronized boolean requiresCompaction(int level) {
        if (level < 0 || level >= depth()) {
            return false;
        }
        long compactingForLevel = new ArrayList<>(compacting).stream().filter(l -> l.level() == level).count();
        int levelSize = items.get(level).size();
        return levelSize - compactingForLevel >= maxItemsPerLevel;
    }

    public synchronized void add(int level, L segment) {
        items.compute(level, (k, v) -> {
            List<L> segments = v == null ? new ArrayList<>(maxItemsPerLevel) : v;
            segments.add(segment);
            return segments;
        });
    }

    public void remove(int level, List<L> segments) {
        items.computeIfPresent(level, (k, v) -> {
            for (L segment : segments) {
                v.remove(segment);
                compacting.remove(segment);
            }
            return v;
        });
    }

    public synchronized List<L> segmentsForCompaction(int level) {
        List<L> toBeCompacted = new ArrayList<>();
        int numSegments = 0;
        for(L segment : segments(level)) {
            if(!compacting.contains(segment)) {
                toBeCompacted.add(segment);
            }
            if(numSegments++ > maxItemsPerLevel) {
                break;
            }
        }
        compacting.addAll(toBeCompacted);
        return toBeCompacted;
    }

    public L current() {
        return current;
    }

    public Iterator<L> segments(Order order) {
        LinkedList<L> flattened = items.values().stream().flatMap(Collection::stream).collect(Collectors.toCollection(LinkedList::new));
        if (Order.OLDEST.equals(order)) {
            return flattened.descendingIterator();
        }
        return flattened.iterator();
    }

    @Override
    public String toString() {
        return "Levels{" + "depth=" + depth() + ", items=" + items + '}';
    }
}
