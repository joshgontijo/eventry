package io.joshworks.fstore.store;


import io.joshworks.fstore.page.Page;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class RandomAccessDataStore implements DataStore {

    static int length = 104857600;
    private final RandomAccessFile raf;

    public RandomAccessDataStore(String name) {
        try {
            raf = new RandomAccessFile(new File(name), "rw");
            raf.setLength(length);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    //TODO file.read must be done in a loop, a read may return less than the total bytes of a buffer

    @Override
    public void write(Page page) throws IOException {
//        long position = raf.getFilePointer();
//        raf.write(data.array());
//        return position;
    }

    @Override
    public Page read(int pageId) throws IOException {
//        raf.seek(position);
//        byte[] data = new byte[size];
//        raf.read(data);
//        return data;
        return null;
    }

    @Override
    public void close() {
        try {
            raf.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
