package io.joshworks.fstore.log;


import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.reader.FixedBufferDataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LogSegment<T> implements Log<T> {


    private final Serializer<T> serializer;
    private final Storage storage;
    private final DataReader reader;
    private long position;

    private static final Logger logger = LoggerFactory.getLogger(LogSegment.class);
    private long entryCount;
    private long size;


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
        this.reader = new FixedBufferDataReader(storage, false, 1); //TODO externalize, TODO for validation, should be always 1
//        this.reader = new HeaderLengthDataReader(storage, 1); //TODO externalize, TODO for validation, should be always 1
//        this.reader = new GrowingBufferDataReader(storage, 1024, true, 1); //TODO externalize, TODO for validation, should be always 1
    }

    private void position(long position) {
        this.position = position;
    }

    private void checkIntegrity(long lastKnownPosition) {
        long position = 0;
        try {

            logger.info("Restoring log state and checking consistency until the position {}", lastKnownPosition);
            Scanner<T> scanner = scanner();
            while (scanner.hasNext()) {
                scanner.next();
                position = scanner.position();
            }
        } catch (Exception e) {
            throw new CorruptedLogException("Failed on integrity check", e);
        }
        //TODO - should advance further after the lastKnownPosition ? (probably yes) lastKnownPosition should be used just for checkpoint
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
    public T get(long position) {
        ByteBuffer data = reader.read(position);
        if(data.remaining() == 0) { //EOF
            return null;
        }
        return serializer.fromBytes(data);
    }

    @Override
    public T get(long position, int length) {
        ByteBuffer data = reader.read(position, length);
        if(data.remaining() == 0) { //EOF
            return null;
        }
        return serializer.fromBytes(data);
    }

    @Override
    public long entries() {
        return entryCount;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long append(T data) {
        ByteBuffer bytes = serializer.toBytes(data);

        long recordPosition = position;
        this.position = Log.write(storage, bytes, position);

        entryCount++;
        size += bytes.limit();
        return recordPosition;
    }

    @Override
    public Scanner<T> scanner() {
        return new LogReader<>(reader, serializer);
    }

    @Override
    public Stream<T> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(scanner(), Spliterator.ORDERED), false);
    }

    @Override
    public Scanner<T> scanner(long position) {
        return new LogReader<>(reader, serializer, position);
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
    private static class LogReader<T> implements Scanner<T> {

        private final DataReader reader;
        private final Serializer<T> serializer;
        private T data;
        private boolean completed = false;
        private long position;

        private LogReader(DataReader reader, Serializer<T> serializer) {
            this(reader, serializer, 0);
        }

        private LogReader(DataReader reader, Serializer<T> serializer, long initialPosition) {
            this.reader = reader;
            this.serializer = serializer;
            this.position = initialPosition;
        }

        private T readAndVerify() {
            long currentPos = position;

            ByteBuffer data = reader.read(currentPos);
            if(data.remaining() == 0) { //EOF
                return null;
            }
            position += data.limit();
            return serializer.fromBytes(data);
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
            if (data == null) {
                throw new NoSuchElementException();
            }
            return data;
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
//        dos.writeTo(bytes);
//        dos.close();
//
//        return baos.toByteArray();
//    }

}
