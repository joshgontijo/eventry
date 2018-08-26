package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Utils;
import io.joshworks.fstore.log.reader.FixedBufferDataReader;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class BlockSegmentTest {

    private DefaultBlockSegment<Integer> segment;
    private File testFile;
    private final int blockSize = Size.BLOCK;


    @Before
    public void setUp() throws Exception {
        testFile = Utils.testFile();
        segment = new DefaultBlockSegment<>(new RafStorage(testFile, Size.MEGABYTE.toBytes(10), Mode.READ_WRITE), Serializers.INTEGER, new FixedBufferDataReader(false), "abc", Type.LOG_HEAD, blockSize);
    }

    @After
    public void tearDown() throws Exception {
        IOUtils.closeQuietly(segment);
        Utils.tryDelete(testFile);
    }

    @Test
    public void add() {

        int entriesPerBlock = blockSize / Integer.BYTES;

        long pos = Log.START;
        for (int i = 0; i < entriesPerBlock; i++) {
            long entryPos = segment.append(i);
            assertEquals(pos, entryPos);
        }


    }

    @Test
    public void poller() {
    }
}