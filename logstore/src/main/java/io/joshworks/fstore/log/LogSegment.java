package io.joshworks.fstore.log;


import io.joshworks.fstore.api.Serializer;
import io.joshworks.fstore.utils.IOUtils;
import io.joshworks.fstore.utils.io.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Iterator;

public class LogSegment<T> implements Log<T> {


    private static final int HEADER_SIZE = Integer.BYTES + Integer.BYTES; //length + checksum

    private final Serializer<T> serializer;
    private final Storage storage;
    private long position;

    private static final Logger logger = LoggerFactory.getLogger(LogSegment.class);


    public static <T> LogSegment<T> create(Storage storage, Serializer<T> serializer) {
       return new LogSegment<>(storage, serializer);
    }

    public static <T> LogSegment<T> open(Storage storage, Serializer<T> serializer, long position) {
        return open(storage, serializer, position, false);
    }

    public static <T> LogSegment<T> open(Storage storage, Serializer<T> serializer, long position, boolean checkIntegrity) {
        LogSegment<T> appender = null;
        try {

            appender = new LogSegment<>(storage, serializer);
            if (checkIntegrity) {
                appender.checkIntegrity(position);
            }
            appender.position(position);
            return appender;
        } catch (CorruptedLogException e) {
            IOUtils.closeQuietly(appender);
            throw e;
        }
    }

    private LogSegment(Storage storage, Serializer<T> serializer) {
        this.serializer = serializer;
        this.storage = storage;
    }

    private void position(long position) {
        this.position = position;
    }

    private void checkIntegrity(long lastKnownPosition) {
        long position = 0;
        logger.info("Restoring log state and checking consistency until the position {}", lastKnownPosition);
        Reader<T> reader = reader();
        while (reader.hasNext()) {
            reader.next();
            position = reader.position();
        }
        if (position != lastKnownPosition) {
            throw new CorruptedLogException(MessageFormat.format("Expected last position {0}, got {1}", lastKnownPosition, position));
        }
        logger.info("Log state restored, current position {}", position);
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public long append(T data) {
        ByteBuffer bytes = serializer.toBytes(data);

        long recordPosition = position;
        ByteBuffer bb = ByteBuffer.allocate(HEADER_SIZE + bytes.remaining());
        bb.putInt(bytes.remaining());
        bb.putInt(Checksum.checksum(bytes));
        bb.put(bytes);

        bb.flip();
        position += storage.write(position, bb);

        return recordPosition;
    }

    @Override
    public Reader<T> reader() {
        return new LogReader<>(storage, serializer);
    }

    @Override
    public Reader<T> reader(long position) {
        return new LogReader<>(storage, serializer, position);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(storage);
    }

    @Override
    public void flush() throws IOException {
        storage.flush();
    }

    //NOT THREAD SAFE
    private static class LogReader<T> implements Reader<T> {

        private final Storage storage;
        private final Serializer<T> serializer;
        private T data;
        private boolean completed = false;
        private long position;

        private LogReader(Storage storage, Serializer<T> serializer) {
            this(storage, serializer, 0);
        }

        private LogReader(Storage storage, Serializer<T> serializer, long initialPosition) {
            this.storage = storage;
            this.serializer = serializer;
            this.position = initialPosition;
        }

        private T readAndVerify() {
            long currentPos = position;
            if (currentPos + HEADER_SIZE > storage.size()) {
                return null;
            }

            //TODO read may return less than the actual dataBuffer size / or zero ?
            ByteBuffer heading = ByteBuffer.allocate(HEADER_SIZE);
            currentPos += storage.read(currentPos, heading);

            heading.flip();
            int length = heading.getInt();
            if (length <= 0) {
                return null;
            }
            if (currentPos + length > storage.size()) {
                throw new IllegalStateException("Not data to be read");
            }

            int writeChecksum = heading.getInt();

            ByteBuffer dataBuffer = ByteBuffer.allocate(length);
            currentPos += storage.read(currentPos, dataBuffer);
            position = currentPos;

            dataBuffer.flip();

            int readChecksum = Checksum.checksum(dataBuffer);
            if (readChecksum != writeChecksum) {
                throw new CorruptedLogException("Checksum verification failed");
            }

            return serializer.fromBytes(dataBuffer);
        }

        @Override
        public boolean hasNext() {
            if (completed) {
                return false;
            }
            this.data = readAndVerify();
            this.completed = this.data == null;
            return !completed;

        }

        @Override
        public T next() {
            return this.data;
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public Iterator<T> iterator() {
            return this;
        }
    }


//    public static void main(String[] args) {
//        int[] a = new int[]{1,2,4,6,7,10,15};
//        int[] b = new int[]{2,5,6,8,11,17,25};
//
//        System.out.println(Arrays.toString(merge(a, b)));
//
//    }
//
//    public static int[] merge(int[] a, int[] b) {
//
//        int[] answer = new int[a.length + b.length];
//        int i = 0, j = 0, k = 0;
//
//        while (i < a.length && j < b.length)
//            answer[k++] = a[i] < b[j] ? a[i++] : b[j++];
//
//        if(i < a.length)
//            System.arraycopy(a, i, answer, k, a.length - i);
//
//        if(j < b.length)
//            System.arraycopy(b, j, answer, k, b.length - j);
//
////        while (i < a.length)
////            answer[k++] = a[i++];
////
////        while (j < b.length)
////            answer[k++] = b[j++];
//
//        return answer;
//    }

    //    protected byte[] writeData(Serializer<T> serializer, T data) throws IOException {
//
//
//        //compressed
//        ByteArrayOutputStream baos = new ByteArrayOutputStream(bytes.length);
//        DeflaterOutputStream dos = new DeflaterOutputStream(baos, new Deflater(Deflater.BEST_SPEED));
//        dos.write(bytes);
//        dos.close();
//
//        return baos.toByteArray();
//    }

}
