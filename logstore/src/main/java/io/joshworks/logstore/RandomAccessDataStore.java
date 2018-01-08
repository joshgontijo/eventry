package io.joshworks.logstore;


import io.joshworks.fstore.utils.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class RandomAccessDataStore implements LogStorage {

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

    @Override
    public synchronized long append(byte[] data) throws IOException {
        long position = raf.getFilePointer();
        raf.write(data);
        return position;
    }

    @Override
    public void read(byte[] data, int offset, int length) throws IOException {
        raf.seek(offset);
        IOUtils.readFully(raf, data);
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
