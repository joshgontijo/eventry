package io.joshworks.fstore.utils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class TSTest {

}

class R implements Runnable {
    FileChannel out;
    ByteBuffer data;

    R(FileChannel out, byte[] data) {
        this.out = out;
        this.data = ByteBuffer.wrap(data);
    }

    public void run() {
        try {
            for (int i = 0; i < 10000; i++) {
                out.write(data);
                data.rewind();
            }


        } catch (IOException e) { System.out.println(e); }
    }
}

class P implements Runnable {
    FileChannel out;

    P(FileChannel out) {
        this.out = out;
    }

    public void run() {
//        try {
//            for (int i = 0; i < 10000; ++i) {
//                long pos = ThreadLocalRandom.current().nextLong(0, out.length() -1);
//                out.position(pos);
//            }
//        } catch (IOException e) { System.out.println(e); }
    }
}

class Test {
    public static void main(String[] args) throws IOException {
        String fname = "test.dat";
        RandomAccessFile raf = new RandomAccessFile(fname, "rw");

        FileChannel channel = raf.getChannel();
        Runnable r1 = new R(channel, "|OOOOOOOOOOOOOOOOO|".getBytes());
        Runnable r2 = new R(channel, "|XXXXXXXXXXXXXXXXX|".getBytes());
        Runnable r3 = new P(channel);

        Thread t1 = new Thread(r1);
        Thread t2 = new Thread(r2);
        Thread t3 = new Thread(r3);


        ByteBuffer bb = ByteBuffer.wrap("|AAA|".getBytes());
        channel.write(bb);


        channel.close();
        channel = new RandomAccessFile(fname, "rw").getChannel();
        channel.position(channel.size());


        channel.write(ByteBuffer.wrap("|BBB|".getBytes()));





//        t1.start();
//        t2.start();
//        t3.start();

    }
}
