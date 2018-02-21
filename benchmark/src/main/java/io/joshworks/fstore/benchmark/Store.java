package io.joshworks.fstore.benchmark;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.zip.CRC32;

public class Store implements Closeable, Flushable {

    private final FileChannel channel;
    private final RandomAccessFile raf;


    public Store(File file) throws Exception {
        this.raf = new RandomAccessFile(file, "rw");
        this.raf.setLength(1024);
        this.channel = raf.getChannel();
    }


    public static void main(String[] args) throws Exception {

        withPosition();
//        withoutPosition();


    }

    public static void withPosition() throws Exception {
        ByteBuffer data = ByteBuffer.allocate(UUID.randomUUID().toString().length());
        try (Store store = new Store(new File("test.f"))) {
            //write
            long writePos = 0;
            long start = System.currentTimeMillis();
            for (int i = 0; i < 1000000; i++) {
                data.clear();
                data.put(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
                data.flip();
                writePos += store.write(data, writePos);
            }
            store.flush();
            System.out.println("WRITE: " + (System.currentTimeMillis() - start));

            //read

            long readPos = 0;
            start = System.currentTimeMillis();
            long count = 0;
            do {
                ByteBuffer dataRead = ByteBuffer.allocate(data.capacity());
                ByteBuffer checksum = ByteBuffer.allocate(Long.BYTES);
//                dataRead.clear();
                readPos += store.read(checksum, readPos);
                checksum.flip();
                long cs = checksum.getLong();

                readPos += store.read(dataRead, readPos);

                CRC32 crc32 = new CRC32();
                crc32.update(SEED);
                crc32.update(dataRead.array());
                long crc = crc32.getValue();
                if(cs != crc)
                    throw new RuntimeException();


                count++;
//                System.out.println(new String(dataRead.array(), StandardCharsets.UTF_8));
            } while (readPos < writePos);
            System.out.println("READ: " + (System.currentTimeMillis() - start) + "  -> " + (count - 1));

        }
    }

    public static void withoutPosition() throws Exception {
        ByteBuffer data = ByteBuffer.allocate(UUID.randomUUID().toString().length());
        try (Store store = new Store(new File("test.db"))) {
            //write
            long start = System.currentTimeMillis();
            for (int i = 0; i < 1000000; i++) {
                data.clear();
                data.put(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
                data.flip();
                store.write(data);
            }
            store.flush();
            System.out.println("WRITE: " + (System.currentTimeMillis() - start));

            //read
//            ByteBuffer dataRead = ByteBuffer.allocate(data.capacity() + Long.BYTES);
            start = System.currentTimeMillis();
            store.channel.position(0);
            for (int i = 0; i < 1000000; i++) {
            ByteBuffer dataRead = ByteBuffer.allocate(data.capacity() + Long.BYTES);
                store.read(dataRead);
//                dataRead.clear();
//                System.out.println(new String(dataRead.array(), StandardCharsets.UTF_8));
            }
            System.out.println("READ: " + (System.currentTimeMillis() - start));

        }
    }

    public long write(ByteBuffer buffer) throws IOException {
        return channel.write(buffer);
    }

    private static final byte[] SEED = ByteBuffer.allocate(8).putLong(1234567890L).array();

    public long write(ByteBuffer buffer, long position) throws IOException {
        CRC32 crc32 = new CRC32();
        crc32.update(SEED);
        crc32.update(buffer.array());
        long crc = crc32.getValue();

        ByteBuffer byteBuffer = ByteBuffer.allocate(buffer.remaining() + Long.BYTES);
        byteBuffer.putLong(crc);
        byteBuffer.put(buffer);
        byteBuffer.flip();

        return channel.write(byteBuffer, position);
    }

    public long read(ByteBuffer buffer) throws IOException {
        return channel.read(buffer);
    }

    public long read(ByteBuffer buffer, long position) throws IOException {
        return channel.read(buffer, position);
    }

    //    private ByteBuffer compress(ByteBuffer buffer) {
//        ByteBuffer dst = ByteBuffer.allocate(buffer.capacity());
//        factory.fastCompressor().compress(buffer, dst);
//        dst.rewind();
//        return dst;
//    }
//
//    private ByteBuffer decompress(ByteBuffer buffer) {
//        buffer.rewind();
//        ByteBuffer allocate = ByteBuffer.allocate(buffer.capacity());
//        factory.fastDecompressor().decompress(buffer.array(), allocate.array());
//        allocate.rewind();
//        return allocate;
//    }
    public void flush() throws IOException {
        channel.force(true);
    }

    public void close() throws IOException {
        flush();
        channel.close();
        raf.close();
    }
}