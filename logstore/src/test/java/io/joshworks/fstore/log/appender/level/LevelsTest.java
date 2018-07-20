package io.joshworks.fstore.log.appender.level;

import io.joshworks.fstore.core.util.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.Order;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentState;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class LevelsTest {

    @Test
    public void segments_return_only_level_segments() {
        Levels<String, DummySegment> levels = Levels.create(3, Arrays.asList(new DummySegment(0)));

        DummySegment seg1 = new DummySegment(1);
        DummySegment seg11 = new DummySegment(1);
        DummySegment seg2 = new DummySegment(1);
        DummySegment seg3 = new DummySegment(1);

        levels.add(1, seg1);
        levels.add(1, seg11);
        levels.add(2, seg2);
        levels.add(3, seg3);


        List<DummySegment> segments = levels.segments(1);
        assertEquals(2, segments.size());
        assertEquals(seg1, segments.get(0));
        assertEquals(seg11, segments.get(1));

        segments = levels.segments(2);
        assertEquals(1, segments.size());
        assertEquals(seg2, segments.get(0));

        segments = levels.segments(3);
        assertEquals(1, segments.size());
        assertEquals(seg3, segments.get(0));


    }

    @Test
    public void segments_return_sorted_for_single_level() {
        Levels<String, DummySegment> levels = Levels.create(3, Arrays.asList(new DummySegment(0)));

        DummySegment seg1 = new DummySegment(1);
        DummySegment seg2 = new DummySegment(1);
        DummySegment seg3 = new DummySegment(1);

        levels.add(1, seg1);
        levels.add(1, seg2);
        levels.add(1, seg3);

        List<DummySegment> segments = levels.segments(1);
        assertEquals(3, segments.size());

        assertEquals(seg1, segments.get(0));
        assertEquals(seg2, segments.get(1));
        assertEquals(seg3, segments.get(2));
    }

    @Test
    public void segments_return_OLDEST_segments_order_for_multiple_levels() {
        DummySegment zero = new DummySegment(0);
        Levels<String, DummySegment> levels = Levels.create(3, Arrays.asList(zero));

        DummySegment seg1 = new DummySegment(1);
        DummySegment seg2 = new DummySegment(2);
        DummySegment seg3 = new DummySegment(3);

        levels.add(1, seg1);
        levels.add(2, seg2);
        levels.add(3, seg3);

        Iterator<DummySegment> it = levels.segments(Order.OLDEST);

        List<DummySegment> segments = Iterators.toList(it);

        assertEquals(4, segments.size()); //zero included

        assertEquals(seg3, segments.get(0));
        assertEquals(seg2, segments.get(1));
        assertEquals(seg1, segments.get(2));
        assertEquals(zero, segments.get(3));
    }

    @Test
    public void segments_return_OLDEST_ordered_segments_on_multiple_levels() {
        DummySegment zero = new DummySegment(0);
        Levels<String, DummySegment> levels = Levels.create(3, Arrays.asList(zero));

        DummySegment seg11 = new DummySegment("seg11", 1);
        DummySegment seg12 = new DummySegment("seg12", 1);
        DummySegment seg21 = new DummySegment("seg21", 2);
        DummySegment seg22 = new DummySegment("seg22", 2);
        DummySegment seg31 = new DummySegment("seg31", 3);
        DummySegment seg32 = new DummySegment("seg32", 3);

        levels.add(1, seg11);
        levels.add(1, seg12);
        levels.add(2, seg21);
        levels.add(2, seg22);
        levels.add(3, seg31);
        levels.add(3, seg32);

        Iterator<DummySegment> it = levels.segments(Order.OLDEST);

        List<DummySegment> segments = Iterators.toList(it);

        assertEquals(7, segments.size()); //zero included

        assertEquals(seg31, segments.get(0));
        assertEquals(seg32, segments.get(1));
        assertEquals(seg21, segments.get(2));
        assertEquals(seg22, segments.get(3));
        assertEquals(seg11, segments.get(4));
        assertEquals(seg12, segments.get(5));
        assertEquals(zero, segments.get(6));
    }

    @Test
    public void segments_return_NEWEST_ordered_segments_on_multiple_levels() {
        DummySegment zero = new DummySegment(0);
        Levels<String, DummySegment> levels = Levels.create(3, Arrays.asList(zero));

        DummySegment seg11 = new DummySegment("seg11", 1);
        DummySegment seg12 = new DummySegment("seg12", 1);
        DummySegment seg21 = new DummySegment("seg21", 2);
        DummySegment seg22 = new DummySegment("seg22", 2);
        DummySegment seg31 = new DummySegment("seg31", 3);
        DummySegment seg32 = new DummySegment("seg32", 3);

        levels.add(1, seg11);
        levels.add(1, seg12);
        levels.add(2, seg21);
        levels.add(2, seg22);
        levels.add(3, seg31);
        levels.add(3, seg32);

        Iterator<DummySegment> it = levels.segments(Order.NEWEST);

        List<DummySegment> segments = Iterators.toList(it);

        assertEquals(7, segments.size()); //zero included

        assertEquals(zero, segments.get(0));
        assertEquals(seg12, segments.get(1));
        assertEquals(seg11, segments.get(2));
        assertEquals(seg22, segments.get(3));
        assertEquals(seg21, segments.get(4));
        assertEquals(seg32, segments.get(5));
        assertEquals(seg31, segments.get(6));
    }


    private static final class DummySegment implements Log<String> {

        private final int level;
        private final String name;

        private DummySegment(int level) {
            this(UUID.randomUUID().toString().substring(0, 8), level);
        }

        private DummySegment(String name, int level) {
            this.level = level;
            this.name = name;
        }

        @Override
        public String name() {
            return null;
        }

        @Override
        public LogIterator<String> iterator() {
            return null;
        }

        @Override
        public Stream<String> stream() {
            return null;
        }

        @Override
        public LogIterator<String> iterator(long position) {
            return null;
        }

        @Override
        public long position() {
            return 0;
        }

        @Override
        public String get(long position) {
            return null;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public SegmentState rebuildState(long lastKnownPosition) {
            return null;
        }

        @Override
        public void delete() {

        }

        @Override
        public void roll(int level) {

        }

        @Override
        public boolean readOnly() {
            return false;
        }

        @Override
        public int entries() {
            return 0;
        }

        @Override
        public int level() {
            return 0;
        }

        @Override
        public long created() {
            return 0;
        }

        @Override
        public long append(String data) {
            return 0;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void flush() throws IOException {

        }

        @Override
        public String toString() {
            return "level=" + level + ", name='" + name + '\'';
        }
    }


}