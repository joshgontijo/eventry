package io.joshworks.fstore.benchmark;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.io.DiskStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Scanner;
import io.joshworks.fstore.log.appender.BlockSegmentBuilder;
import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.serializer.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class LogAppenderBench {

    //    private static final int SEGMENT_SIZE = 1073741824; //1gb
    private static final int SEGMENT_SIZE = 10485760; //10mb
    private static final int ITEMS = 1000000;

    public static void main(String[] args) throws IOException {
//        raf();
        segmentAppender();
//        blockCompressedSegmentAppender();
    }


    public static void segmentAppender() throws IOException {
        try (LogAppender<String> appender = LogAppender.simpleLog(new Builder<>(new File("appenderBenchSimple"), new StringSerializer()).segmentSize(SEGMENT_SIZE))) {
            long start = System.currentTimeMillis();
            List<Long> vals = new LinkedList<>();
            for (int i = 0; i < ITEMS; i++) {
                vals.add(appender.append("A"));
            }
            System.out.println("SEGMENT_APPENDER_WRITE: " + (System.currentTimeMillis() - start) + "ms");
            appender.flush();

            int i = 0;
            for (Long val : vals) {
                System.out.println("I: " + i++ + " - POS: " + val + " - VAL: " + appender.get(val));
            }


//            start = System.currentTimeMillis();
//            Scanner<String> scanner = appender.scanner(10485728);
//            long i = 0;
//            while (scanner.hasNext()) {
//                String value = scanner.next();
//                long position = scanner.position();
//
//                System.out.println("I: " + i++ + " - POS: " + position + " - VAL: " + value);
//            }
            System.out.println("SEGMENT_APPENDER_READ: " + (System.currentTimeMillis() - start) + "ms");
        }

    }

    public static void blockCompressedSegmentAppender() throws IOException {
        try (LogAppender<String> appender = LogAppender.blockLog(
                new BlockSegmentBuilder<>(new Builder<>(new File("appenderBenchBlock"), new StringSerializer()), new SnappyCodec()))) {

            long start = System.currentTimeMillis();
            for (int i = 0; i < ITEMS; i++) {
                appender.append(UUID.randomUUID().toString());
            }
            System.out.println("BLOCK_SEGMENT_APPENDER_WRITE: " + (System.currentTimeMillis() - start) + "ms");

            appender.flush();

            start = System.currentTimeMillis();
            Scanner<String> scanner = appender.scanner();
            while (scanner.hasNext()) {
                String value = scanner.next();
//            System.out.println(value);
            }
            System.out.println("BLOCK_SEGMENT_APPENDER_READ: " + (System.currentTimeMillis() - start) + "ms");
        }

    }

    public static void raf() throws IOException {
        Storage storage = new DiskStorage(new File("diskStore.f"));

        long start = System.currentTimeMillis();
        long writePos = 0;
        for (int i = 0; i < ITEMS; i++) {
            writePos += storage.write(writePos, ByteBuffer.wrap(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)));
        }
        System.out.println("STORAGE_RAF_WRITE: " + (System.currentTimeMillis() - start) + "ms");

        storage.flush();

        start = System.currentTimeMillis();
        long readPos = 0;
        long counter = 0;
        while (counter++ <= ITEMS) {
            ByteBuffer buffer = ByteBuffer.allocate(36);
            int read = storage.read(readPos, buffer);
            if (read == 0) {
                throw new RuntimeException();
            }
            readPos += read;


            buffer.flip();
//            last = new String(buffer.array(), StandardCharsets.UTF_8);
//            System.out.println(new String(buffer.array(), StandardCharsets.UTF_8));
        }
        System.out.println("STORAGE_RAF_READ: " + (System.currentTimeMillis() - start) + "ms");
    }
}
