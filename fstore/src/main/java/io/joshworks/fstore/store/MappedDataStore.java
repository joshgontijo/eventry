package io.joshworks.fstore.store;


import io.joshworks.fstore.page.Page;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MappedDataStore implements DataStore {

    static int length = 104857600;
    private final RandomAccessFile raf;
    private MappedByteBuffer mbb;

    public MappedDataStore(String name) {
        try {
            raf = new RandomAccessFile(new File(name), "rw");
            raf.setLength(length);
            mbb = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, length);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    @Override
    public void write(Page page) {
//        long position = mbb.position();
//        mbb.put(data);
//        return position;
    }

    @Override
    public Page read(int pageId) {

//        int original = mbb.position();
//        short recordSize = mbb.getShort();
//
//        byte[] data = new byte[recordSize];
//
//        mbb.get(data);
//        mbb.position(original);
//        return data;
        return null;
    }

    @Override
    public void close() {
        try {
            mbb.force();
            raf.close();
            mbb = null;
            System.gc();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
