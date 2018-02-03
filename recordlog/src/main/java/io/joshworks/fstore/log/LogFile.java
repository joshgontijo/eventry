package io.joshworks.fstore.log;


import io.joshworks.fstore.api.Serializer;
import io.joshworks.fstore.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

public class LogFile<T> implements Writer<T>, Closeable {

    private static final byte[] CRC_SEED = ByteBuffer.allocate(4).putInt(456765723).array();
    private static final int HEADING_SIZE = Integer.BYTES + Integer.BYTES; //length + checksum
    private static final int DEFAULT_BUFFER_SIZE = 2048;

    private final Serializer<T> serializer;
    private final FileChannel channel;
    private final ByteBuffer buffer;
    private final RandomAccessFile raf;

    //Not using channel's position since we want the position
    private final AtomicLong position = new AtomicLong();

    private static final Logger logger = LoggerFactory.getLogger(LogFile.class);


    public static <T> LogFile<T> create(File file, Serializer<T> serializer, long fileSize) {
        return create(file, serializer, fileSize, DEFAULT_BUFFER_SIZE);
    }

    public static <T> LogFile<T> create(File file, Serializer<T> serializer, long fileSize, int bufferSize) {
        LogFile<T> appender = null;
        try {
            appender = new LogFile<>(file, serializer, bufferSize);
            appender.fileSize(fileSize);
            return appender;
        } catch (Exception e) {
            IOUtils.closeQuietly(appender);
            throw e;
        }
    }

    public static <T> LogFile<T> open(File file, Serializer<T> serializer, int bufferSize, long position) {
        return open(file, serializer, bufferSize, position, false);
    }

    public static <T> LogFile<T> open(File file, Serializer<T> serializer, int bufferSize, long position, boolean checkConsistency) {
        LogFile<T> appender = null;
        try {
            appender = new LogFile<>(file, serializer, bufferSize);
            if (checkConsistency) {
                appender.checkConsistency(position);
            }
            appender.position(position);
            return appender;
        } catch (Exception e) {
            IOUtils.closeQuietly(appender);
            throw e;
        }
    }

    private LogFile(File file, Serializer<T> serializer, int bufferSize) {
        try {
            if (bufferSize < HEADING_SIZE)
                throw new IllegalArgumentException(MessageFormat.format("bufferSize must be greater or equals to {0}", HEADING_SIZE));

            this.serializer = serializer;
            this.raf = new RandomAccessFile(file, "rw");
            this.channel = raf.getChannel();

            this.buffer = ByteBuffer.allocate(bufferSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void position(long position) {
        try {
            this.position.set(position);
            this.channel.position(position);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void fileSize(long size) {
        try {
            raf.setLength(size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkConsistency(long lastKnownPosition) {
        try {
            logger.info("Restoring log state and checking consistency until the position {}", lastKnownPosition);
            Reader<T> reader = reader();
            long position = 0;
            while (reader.hasNext() && position < lastKnownPosition) {
                reader.next();
                position = reader.position();
            }
            if (position != lastKnownPosition) {
                throw new CorruptedLogException(MessageFormat.format("Expected last position {0}, got {1}", lastKnownPosition, position));
            }
            logger.info("Log state restored, current position {}", position);
        } catch (Exception e) {
            throw new CorruptedLogException("Inconsistent log state found while restoring state", e);
        }
    }

    public long position() {
        if (!this.channel.isOpen()) {
            throw new RuntimeException("Appender is closed");
        }
        return position.get();
    }

    @Override
    public long write(T data) {
        try {

            byte[] bytes = serializer.toBytes(data);

            long recordPosition = channel.position();
            buffer.putInt(bytes.length);
            buffer.putInt(checksum(bytes));

            //------------- NOT THREAD SAFE -------------
            IOUtils.writeFully(channel, buffer, bytes);
            //update the position only after fully inserted the data
            this.position.set(channel.position());
            //-------------------------------------------
            return recordPosition;

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            buffer.clear();
        }
    }

    private static int checksum(byte[] data) {
        return checksum(data, 0, data.length);
    }

    private static int checksum(ByteBuffer buffer) {
        if (!buffer.hasArray()) {
            throw new IllegalArgumentException("ByteBuffer must be an array backed buffer");
        }
        return checksum(buffer.array(), 0, buffer.limit());
    }

    private static int checksum(byte[] data, int offset, int length) {
        final CRC32 checksum = new CRC32();
        checksum.update(CRC_SEED);
        checksum.update(data, offset, length);
        return (int) checksum.getValue();
    }


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

    public Reader<T> reader() {
        return new LogReader<>(channel, serializer);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(channel);
        IOUtils.closeQuietly(raf);
    }

    //NOT THREAD SAFE
    private static class LogReader<T> implements Reader<T> {

        private final FileChannel channel;
        private final Serializer<T> serializer;
        private T data;
        private boolean completed = false;
        private long position;

        private LogReader(FileChannel channel, Serializer<T> serializer) {
            this(channel, serializer, 0);
        }

        private LogReader(FileChannel channel, Serializer<T> serializer, long initialPosition) {
            this.channel = channel;
            this.serializer = serializer;
            this.position = initialPosition;
        }

        private T readAndVerify() throws IOException {
            long currentPos = position;
            if (currentPos + HEADING_SIZE > channel.size()) {
                return null;
            }

            //TODO read may return less than the actual dataBuffer size / or zero ?
            ByteBuffer heading = ByteBuffer.allocate(HEADING_SIZE);
            currentPos += IOUtils.readFully(channel, currentPos, heading);

            heading.flip();
            int length = heading.getInt();
            if (length <= 0) {
                return null;
            }
            if (currentPos + length > channel.size()) {
                throw new IllegalStateException("Not data to be read");
            }

            int writeChecksum = heading.getInt();

            ByteBuffer dataBuffer = ByteBuffer.allocate(length);
            currentPos += IOUtils.readFully(channel, currentPos, dataBuffer);
            position = currentPos;

            dataBuffer.flip();

            int readChecksum = checksum(dataBuffer);
            if (readChecksum != writeChecksum) {
                throw new IllegalStateException("Corrupted data");
            }

            return serializer.fromBytes(dataBuffer.array(), 0, dataBuffer.limit());
        }

        @Override
        public boolean hasNext() {
            if (completed) {
                return false;
            }
            try {
                this.data = readAndVerify();
                this.completed = this.data == null;
                return !completed;

            } catch (Exception e) {
                this.completed = true;
                throw new RuntimeException(e);
            }
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

}
