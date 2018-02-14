package io.joshworks.fstore.core;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.utils.MockFileChannel;
import io.joshworks.fstore.core.utils.SingleByteMockFileChannel;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IOUtilsTest {

    @Test
    public void writeFully() throws IOException {
        int dataSize = 50;

        MockFileChannel channel = new MockFileChannel();

        byte[] data = sequentialBytes(dataSize);
        ByteBuffer bb = ByteBuffer.wrap(data);

        IOUtils.writeFully(channel, bb, data);

        assertEquals(dataSize, channel.received.size());
        assertTrue(Arrays.equals(data, channel.received.toByteArray()));

    }

    @Test
    public void writeFullyTo() throws IOException {

        byte[] data = sequentialBytes(50);
        ByteBuffer bb = ByteBuffer.wrap(data);
        SingleByteMockFileChannel channel = new SingleByteMockFileChannel();

        IOUtils.writeFully(channel, bb);

        assertEquals(data.length, channel.received.size());
        assertTrue(Arrays.equals(data, channel.received.toByteArray()));
    }

    @Test
    public void readFully_full_buffer() throws IOException {

        byte[] testData = sequentialBytes(50);
        MockFileChannel channel = new MockFileChannel();
        channel.write(ByteBuffer.wrap(testData));

        ByteBuffer dst = ByteBuffer.allocate(50);
        IOUtils.readFully(channel, dst);

        assertTrue(Arrays.equals(testData, dst.array()));
    }

    @Test
    public void readFully_split_read() throws IOException {
        int size = 50;

        byte[] testData = sequentialBytes(size);
        SingleByteMockFileChannel channel = new SingleByteMockFileChannel();
        channel.received.write(testData);

        ByteBuffer dst = ByteBuffer.allocate(size);
        IOUtils.readFully(channel, dst);

        assertTrue(Arrays.equals(testData, dst.array()));
    }

    private byte[] sequentialBytes(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) i;
        }
        return data;
    }
}