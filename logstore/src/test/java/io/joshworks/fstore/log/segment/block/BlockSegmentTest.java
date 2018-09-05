package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.Utils;
import io.joshworks.fstore.log.reader.FixedBufferDataReader;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BlockSegmentTest {

    private DefaultBlockSegment<Integer> segment;
    private File testFile;
    private final int blockSize = 4096;

    @Before
    public void setUp() {
        testFile = Utils.testFile();
        segment = new DefaultBlockSegment<>(new RafStorage(testFile, Size.MEGABYTE.toBytes(10), Mode.READ_WRITE), Serializers.INTEGER, new FixedBufferDataReader(false), "abc", Type.LOG_HEAD, blockSize);
    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(segment);
        Utils.tryDelete(testFile);
    }

    @Test
    public void add_returns_same_position_for_same_block() {
        int entriesPerBlock = blockSize / Integer.BYTES;

        long pos = Log.START;
        for (int i = 0; i < entriesPerBlock; i++) {
            long entryPos = segment.append(i);
            assertEquals("Failed on " + i, pos, entryPos);
        }

        long entryPos = segment.append(123);
        assertTrue(entryPos > pos);
    }

    @Test
    public void entries_returns_flushed_blocks() {
        assertEquals(0, segment.entries());
        segment.append(123);
        segment.flush();
        assertEquals(1, segment.entries());
    }

    @Test
    public void stream_returns_all_data() {
        int entriesPerBlock = blockSize / Integer.BYTES;
        IntStream.range(0, entriesPerBlock).forEach(segment::append);

        segment.flush();

        long count = segment.stream(Direction.FORWARD).count();
        assertEquals(entriesPerBlock, count);
    }

    @Test(expected = IllegalStateException.class)
    public void getBlock_throws_exception_of_no_block_is_present() {
        segment.getBlock(Log.START);
    }

    @Test
    public void getBlock_returns_correct_block() {
        long position = segment.append(123);
        segment.flush();

        Block<Integer> found = segment.getBlock(position);
        assertNotNull(found);
        assertEquals(1, found.entryCount());
        assertEquals(Integer.valueOf(123), found.get(0));
    }

    @Test
    public void poller_take_returns_all_persisted_data() throws IOException, InterruptedException {
        int entriesPerBlock = blockSize / Integer.BYTES;
        IntStream.range(0, entriesPerBlock).forEach(segment::append);

        segment.flush();

        try (PollingSubscriber<Integer> poller = segment.poller()) {
            for (int i = 0; i < entriesPerBlock; i++) {
                int val = poller.take();
                assertEquals(i, val);
            }
        }
    }

    @Test
    public void poller_poll_returns_all_persisted_data() throws IOException, InterruptedException {
        int entriesPerBlock = blockSize / Integer.BYTES;
        IntStream.range(0, entriesPerBlock).forEach(segment::append);

        segment.flush();

        try (PollingSubscriber<Integer> poller = segment.poller()) {
            for (int i = 0; i < entriesPerBlock; i++) {
                int val = poller.poll();
                assertEquals(i, val);
            }
        }
    }

    @Test
    public void poller_poll_doesnt_return_non_persisted_data() throws IOException, InterruptedException {
        segment.append(123);
        segment.append(456);

        try (PollingSubscriber<Integer> poller = segment.poller()) {
            Integer poll = poller.poll();
            assertNull(poll);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void get_is_not_supported() {
        segment.get(123);
    }

    @Test
    public void endOfLog_only_when_queue_is_empty_and_closed_was_called() throws IOException, InterruptedException {
        segment.append(123);
        segment.append(456);

        PollingSubscriber<Integer> poller = segment.poller();
        poller.close();
        assertFalse(poller.endOfLog());
        poller.poll();
        assertFalse(poller.endOfLog());
        poller.poll();
        assertFalse(poller.endOfLog());

    }
}