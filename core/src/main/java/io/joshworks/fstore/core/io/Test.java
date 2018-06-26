package io.joshworks.fstore.core.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Test {
    public static void main(String[] args) throws IOException {
        int blockSize = 2048 * 1024;
        int numberOfBlocks = 200;
        int fileLength = numberOfBlocks * blockSize;
        RandomAccessFile raf = new RandomAccessFile("test.dat", "rw");
        raf.setLength(fileLength);
        int pos = (numberOfBlocks - 1) * blockSize;
        int size = (int) Math.min(blockSize, fileLength - pos);
        MappedByteBuffer mbb = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, pos, size);
        System.out.println("Write region 0x" + Long.toHexString(pos) + "..+0x" + Long.toHexString(size));
        for (int k = 0; k < mbb.limit(); k++)
            mbb.put(k, (byte) 65);
        System.out.println("Force");
        mbb.force();
        System.out.println("OK");
        raf.close();
    }
}