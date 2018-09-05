package io.joshworks.fstore.log.appender.level;

import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.TimeoutReader;
import io.joshworks.fstore.log.Order;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentState;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class LevelsTest {

    @Test
    public void segments_return_only_level_segments() {

        DummySegment zero = new DummySegment(0);
        DummySegment seg1 = new DummySegment(1);
        DummySegment seg11 = new DummySegment(1);
        DummySegment seg2 = new DummySegment(2);
        DummySegment seg3 = new DummySegment(3);

        Levels<String, DummySegment> levels = Levels.create(3,
                Arrays.asList(
                        zero,
                        seg1,
                        seg11,
                        seg2,
                        seg3));


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

        DummySegment zero = new DummySegment(0);
        DummySegment seg1 = new DummySegment(1);
        DummySegment seg2 = new DummySegment(1);
        DummySegment seg3 = new DummySegment(1);

        Levels<String, DummySegment> levels = Levels.create(3,
                Arrays.asList(
                        zero,
                        seg1,
                        seg2,
                        seg3));

        List<DummySegment> segments = levels.segments(1);
        assertEquals(3, segments.size());

        assertEquals(seg1, segments.get(0));
        assertEquals(seg2, segments.get(1));
        assertEquals(seg3, segments.get(2));
    }

    @Test
    public void segments_return_OLDEST_segments_order_for_multiple_levels() {
        DummySegment zero = new DummySegment(0);
        DummySegment seg1 = new DummySegment(1);
        DummySegment seg2 = new DummySegment(2);
        DummySegment seg3 = new DummySegment(3);

        Levels<String, DummySegment> levels = Levels.create(3,
                Arrays.asList(
                        zero,
                        seg1,
                        seg2,
                        seg3));


        Iterator<DummySegment> it = levels.segments(Order.FORWARD);

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
        DummySegment seg11 = new DummySegment("seg11", 1);
        DummySegment seg12 = new DummySegment("seg12", 1);
        DummySegment seg21 = new DummySegment("seg21", 2);
        DummySegment seg22 = new DummySegment("seg22", 2);
        DummySegment seg31 = new DummySegment("seg31", 3);
        DummySegment seg32 = new DummySegment("seg32", 3);

        Levels<String, DummySegment> levels = Levels.create(3,
                Arrays.asList(
                        zero,
                        seg11,
                        seg12,
                        seg21,
                        seg22,
                        seg31,
                        seg32));

        Iterator<DummySegment> it = levels.segments(Order.FORWARD);

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
        DummySegment seg11 = new DummySegment("seg11", 1);
        DummySegment seg12 = new DummySegment("seg12", 1);
        DummySegment seg21 = new DummySegment("seg21", 2);
        DummySegment seg22 = new DummySegment("seg22", 2);
        DummySegment seg31 = new DummySegment("seg31", 3);
        DummySegment seg32 = new DummySegment("seg32", 3);

        Levels<String, DummySegment> levels = Levels.create(3,
                Arrays.asList(
                        zero,
                        seg11,
                        seg12,
                        seg21,
                        seg22,
                        seg31,
                        seg32));

        Iterator<DummySegment> it = levels.segments(Order.BACKWARD);

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

    @Test
    public void get_return_segment_for_given_index() {
        DummySegment zero = new DummySegment(0);
        DummySegment seg11 = new DummySegment("seg11", 1);
        DummySegment seg12 = new DummySegment("seg12", 1);
        DummySegment seg21 = new DummySegment("seg21", 2);
        DummySegment seg22 = new DummySegment("seg22", 2);
        DummySegment seg31 = new DummySegment("seg31", 3);
        DummySegment seg32 = new DummySegment("seg32", 3);

        Levels<String, DummySegment> levels = Levels.create(3,
                Arrays.asList(
                        zero,
                        seg11,
                        seg12,
                        seg21,
                        seg22,
                        seg31,
                        seg32));


        assertEquals(zero, levels.get(6));
        assertEquals(seg12, levels.get(5));
        assertEquals(seg11, levels.get(4));
        assertEquals(seg22, levels.get(3));
        assertEquals(seg21, levels.get(2));
        assertEquals(seg32, levels.get(1));
        assertEquals(seg31, levels.get(0));
    }


    @Test(expected = IllegalArgumentException.class)
    public void exception_is_thrown_when_new_segment_is_not_level_zero() {
        Levels<String, DummySegment> levels = Levels.create(3, Arrays.asList(new DummySegment(0)));
        levels.appendSegment(new DummySegment(1));
    }

    @Test
    public void appending_maintains_ordering() {
        DummySegment seg1 = new DummySegment("seg1", 0);
        DummySegment seg2 = new DummySegment("seg2", 0);
        DummySegment seg3 = new DummySegment("seg3", 0);

        Levels<String, DummySegment> levels = Levels.create(3, Arrays.asList(seg1));

        levels.appendSegment(seg2);

        assertEquals(seg1, levels.get(0));
        assertEquals(1, levels.get(0).level);

        //the new level zero
        assertEquals(seg2, levels.get(1));
        assertEquals(0, levels.get(1).level);


        levels.appendSegment(seg3);

        assertEquals(seg1, levels.get(0));
        assertEquals(1, levels.get(0).level);

        assertEquals(seg2, levels.get(1));
        assertEquals(1, levels.get(1).level);

        //the new level zero
        assertEquals(seg3, levels.get(2));
        assertEquals(0, levels.get(2).level);

    }

    @Test
    public void merge_maintains_ordering() {
        DummySegment seg1 = new DummySegment("seg1", 0);
        DummySegment seg2 = new DummySegment("seg2", 0);
        DummySegment seg3 = new DummySegment("seg3", 0);
        DummySegment seg4 = new DummySegment("seg4", 0);

        DummySegment seg5 = new DummySegment("seg5", 2);

        Levels<String, DummySegment> levels = Levels.create(3, Arrays.asList(seg1));

        levels.appendSegment(seg2);
        levels.appendSegment(seg3);
        levels.appendSegment(seg4);

        levels.merge(Arrays.asList(seg1, seg2, seg3), seg5);


        assertEquals(seg5, levels.get(0));
        assertEquals(2, levels.get(0).level);

        assertEquals(seg4, levels.get(1));
        assertEquals(0, levels.get(1).level);

    }


    private static final class DummySegment implements Log<String> {

        private int level;
        private final String name;
        private final long createdDate;
        private boolean readOnly;

        private DummySegment(int level) {
            this(UUID.randomUUID().toString().substring(0, 8), level);
        }

        private DummySegment(String name, int level) {
            this.level = level;
            this.name = name;
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.createdDate = System.currentTimeMillis();
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
        public LogIterator<String> iterator(Order order) {
            return null;
        }

        @Override
        public LogIterator<String> iterator(long position, Order order) {
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
        public PollingSubscriber<String> poller(long position) {
            return null;
        }

        @Override
        public PollingSubscriber<String> poller() {
            return null;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public Set<TimeoutReader> readers() {
            return new HashSet<>();
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
            this.level = level;
            this.readOnly = true;
        }

        @Override
        public void roll(int level, ByteBuffer footer) {
            this.level = level;
            this.readOnly = true;
        }

        @Override
        public ByteBuffer readFooter() {
            return ByteBuffer.allocate(0);
        }

        @Override
        public boolean readOnly() {
            return readOnly;
        }

        @Override
        public long entries() {
            return 0;
        }

        @Override
        public int level() {
            return level;
        }

        @Override
        public long created() {
            return createdDate;
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