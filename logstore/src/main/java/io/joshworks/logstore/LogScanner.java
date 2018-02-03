package io.joshworks.logstore;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.Consumer;

class LogScanner implements Closeable {

    private final FileChannel channel;

    LogScanner(String name) {
        try {
            channel = new RandomAccessFile(name, "r").getChannel();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void forEach(Consumer<EventEntry> consumer) {
        try {
            int offset = 0;
            int lastRead;
            ByteBuffer offsetData = ByteBuffer.allocate(EventLogStore.INT_SIZE);
            do {
                offsetData.rewind();
                lastRead = channel.read(offsetData, offset);
                if (lastRead > 0) {
                    offset += lastRead;

                    offsetData.flip();
                    int eventSize = offsetData.getInt();

                    ByteBuffer eventData = ByteBuffer.allocate(eventSize);
                    lastRead = channel.read(eventData, offset);

                    eventData.flip();
                    consumer.accept(EventEntry.of(offset, eventData));
                    offset += lastRead;

                }
            } while (lastRead > 0);


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    static class EventEntry {
        public final long position;
        public final ByteBuffer data;

        private EventEntry(long position, ByteBuffer data) {
            this.position = position;
            this.data = data;
        }

        public static EventEntry of(long position, ByteBuffer data) {
            return new EventEntry(position, data);
        }

    }

}
