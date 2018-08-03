package io.joshworks.fstore.log.appender.level;

import io.joshworks.fstore.core.util.Iterators;
import io.joshworks.fstore.log.appender.Order;
import io.joshworks.fstore.log.segment.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Levels<T, L extends Log<T>> {

    private final int maxItemsPerLevel;
    private final List<L> segments = new ArrayList<>();

    private Levels(int maxItemsPerLevel, List<L> segments) {
        this.maxItemsPerLevel = maxItemsPerLevel;

        this.segments.addAll(segments);
        this.segments.sort((o1, o2) -> {
            int levelDiff = o2.level() - o1.level();
            if (levelDiff == 0) {
                int createdDiff = Long.compare(o1.created(), o2.created());
                if (createdDiff != 0)
                    return createdDiff;
            }
            return levelDiff;
        });

        if (this.segments.stream().noneMatch(seg -> seg.level() == 0)) {
            throw new IllegalStateException("Level zero must be present");
        }
    }

    public synchronized List<L> segments(int level) {
        return segments.stream().filter(seg -> seg.level() == level).collect(Collectors.toList());
    }

    public L get(int segmentIdx) {
        return segments.get(segmentIdx);
    }

    public int depth() {
        return segments.stream().mapToInt(Log::level).max().orElse(0);
    }

    public static <T, L extends Log<T>> Levels<T, L> create(int maxItemsPerLevel, List<L> segments) {
        return new Levels<>(maxItemsPerLevel, segments);
    }

    public synchronized void appendSegment(L segment) {
        if (segment.level() != 0) {
            throw new IllegalArgumentException("New segment must be level zero");
        }
        int size = segments.size();
        if (size == 0) {
            segments.add(segment);
            return;
        }
        L prevHead = segments.get(size - 1);

        prevHead.roll(1);
        if (!prevHead.readOnly()) {
            throw new IllegalStateException("Segment must be marked as read only after rolling");
        }

        segments.add(segment);
    }

    public int numSegments() {
        return segments.size();
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

   public int compactionThreshold() {
        return maxItemsPerLevel;
   }

    public synchronized void merge(List<L> segments, L merged) {
        if (segments.isEmpty() || merged == null) {
            return;
        }

        int latestIndex = -1;
        for (L seg : segments) {
            int i = this.segments.indexOf(seg);
            if (i < 0) {
                throw new IllegalStateException("Segment not found: " + seg.name());
            }
            if (latestIndex >= 0 && latestIndex + 1 != i) {
                throw new IllegalArgumentException("Segments to be deleted must be contiguous");
            }
            latestIndex = i;
        }

        int firstIdx = this.segments.indexOf(segments.get(0));
        this.segments.set(firstIdx, merged);
        for (int i = 1; i < segments.size(); i++) {
            this.segments.remove(firstIdx + 1);
        }

    }

    public L current() {
        return segments.get(segments.size() - 1);
    }

    public Iterator<L> segments(Order order) {
        ArrayList<L> copy = new ArrayList<>(segments);
        return Order.OLDEST.equals(order) ?copy.iterator() : Iterators.reversed(copy);
    }

    @Override
    public String toString() {
        return "Levels{" + "depth=" + depth() + ", items=" + Arrays.toString(segments.toArray()) + '}';
    }
}