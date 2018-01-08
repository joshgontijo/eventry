/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package io.joshworks.fstore.utils;


import java.io.IOException;
import java.io.RandomAccessFile;


public class IOUtils {

    private IOUtils() {

    }

    public static void readFully(RandomAccessFile from, byte[] to) throws IOException {
        int totalRead = 0;
        int length = to.length;
        if(length <= 0) {
            throw new IllegalArgumentException("Destination buffer size must be greater than zero");
        }

        while (totalRead < length) {
            int bytesRead = from.read(to, totalRead, to.length - totalRead);
            if (bytesRead < 0) {
                throw new IOException("Data stream ended prematurely");
            }
            totalRead += bytesRead;
        }
    }


    public static void readFully(RandomAccessFile from, byte[] to, int offset, int length) throws IOException {
        int totalRead = 0;
        if(length <= 0) {
            throw new IllegalArgumentException("Destination buffer size must be greater than zero");
        }

        while (totalRead < length) {
            int bytesRead = from.read(to, offset + totalRead, to.length - totalRead);
            if (bytesRead < 0) {
                throw new IOException("Data stream ended prematurely");
            }
            totalRead += bytesRead;
        }
    }

}
