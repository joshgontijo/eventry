/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package io.joshworks.fstore.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


public final class IOUtils {


    private static final Logger logger = LoggerFactory.getLogger(IOUtils.class);

    private IOUtils() {

    }

    public static void writeFully(FileChannel channel, ByteBuffer buffer, byte[] bytes) throws IOException {
        if (bytes.length == 0) {
            return;
        }

        int lastWrittenIdx = 0;
        do {
            int length = Math.min(buffer.remaining(), bytes.length - lastWrittenIdx);
            buffer.put(bytes, lastWrittenIdx, length);
            lastWrittenIdx += length;

            buffer.flip();
            writeFully(channel, buffer);
            buffer.clear();

        } while (lastWrittenIdx < bytes.length);
    }

    public static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        do {
            channel.write(buffer);
        } while (buffer.hasRemaining());
    }

    public static int readFully(FileChannel from, ByteBuffer buffer) throws IOException {
        return readFully(from, 0, buffer);
    }

    public static int readFully(FileChannel from, long offset, ByteBuffer buffer) throws IOException {
        int bytesRead = 0;
        while (buffer.hasRemaining()) {
            bytesRead += from.read(buffer, offset + bytesRead);
            if (bytesRead == -1) {
                throw new IOException("Data stream ended prematurely");
            }
        }
        return bytesRead;
    }



    public static void readFully(RandomAccessFile from, byte[] to) throws IOException {
        int totalRead = 0;
        int length = to.length;
        if (length <= 0) {
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
        if (length <= 0) {
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

    public static void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Exception e) {
            logger.warn("Error while closing resource" , e);
        }
    }
}
