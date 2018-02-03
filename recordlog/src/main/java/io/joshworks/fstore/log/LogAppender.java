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
import java.util.zip.CRC32;

public class LogAppender<T> implements Writer<T>, Closeable {

    private static final byte[] CRC_SEED = ByteBuffer.allocate(4).putInt(456765723).array();
    private static final int HEADING_SIZE = Integer.BYTES + Integer.BYTES; //length + checksum
    private static final int DEFAULT_BUFFER_SIZE = 2048;
    private static final long DEFAULT_FILE_SIZE = 10485760; //10mb

    private final Serializer<T> serializer;
    private final FileChannel channel;
    private final ByteBuffer buffer;
    private final RandomAccessFile raf;

    private static final Logger logger = LoggerFactory.getLogger(LogAppender.class);

    public LogAppender(File file, Serializer<T> serializer) {
        this(file, serializer, DEFAULT_FILE_SIZE, DEFAULT_BUFFER_SIZE);
    }

    public LogAppender(File file, Serializer<T> serializer, long fileSize) {
        this(file, serializer, fileSize, DEFAULT_BUFFER_SIZE);
    }

    public LogAppender(File file, Serializer<T> serializer, long fileSize, int bufferSize) {
        try {
            if (bufferSize < HEADING_SIZE)
                throw new IllegalArgumentException(MessageFormat.format("bufferSize must be greater or equals to {0}", HEADING_SIZE));

            boolean exists = file.exists(); //TODO improve ?

            this.serializer = serializer;
            this.raf = new RandomAccessFile(file, "rw");
            this.channel = raf.getChannel();

            if (exists) {
                //re-read everything and set the position
                long lastPosition = restore();
                channel.position(lastPosition);
            } else {
                raf.setLength(fileSize);
            }

            this.buffer = ByteBuffer.allocate(bufferSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long restore() {
        try {
            logger.info("Restoring log state");
            Reader<T> reader = reader();
            long position = 0;
            while (reader.hasNext()) {
                position = reader.position();
            }
            logger.info("Log state restored, current position {}", position);
            return position;
        } catch (Exception e) {
            throw new IllegalStateException("Inconsistent log state found while restoring state");
        }
    }

    public long position() {
        try {
            return channel.position();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long write(T data) {
        try {

            byte[] bytes = serializer.toBytes(data);

            long position = channel.position();
            buffer.putInt(bytes.length);
            buffer.putInt(checksum(bytes));

            IOUtils.writeFully(channel, buffer, bytes);

            return position;

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
        return new LogReader<>(raf.getChannel(), serializer);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(channel);
        IOUtils.closeQuietly(raf);
    }

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
            if (position + HEADING_SIZE > channel.size()) {
                return null;
            }

            //TODO read may return less than the actual dataBuffer size / or zero ?
            ByteBuffer heading = ByteBuffer.allocate(HEADING_SIZE);
            this.position += IOUtils.readFully(channel, position, heading);

            heading.flip();
            int length = heading.getInt();
            if (length <= 0) {
                return null;
            }
            if (position + length > channel.size()) {
                throw new IllegalStateException("Not data to be read");
            }

            int writeChecksum = heading.getInt();

            ByteBuffer dataBuffer = ByteBuffer.allocate(length);

            this.position += IOUtils.readFully(channel, position, dataBuffer);
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
