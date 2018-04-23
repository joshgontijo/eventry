package io.joshworks.fstore.core.io;


import io.joshworks.fstore.core.RuntimeIOException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DiskStorage extends BaseStorage {

    public DiskStorage(File target) {
        super(target);
    }

    public DiskStorage(File target, long length) {
        super(target, length);
    }

    /**
     * Using channel.write(buffer, position) will result in a pwrite() sys call
     */
    @Override
    public int write(ByteBuffer data) {
        ensureNonEmpty(data);
        try {
            int written = 0;
            while (data.hasRemaining()) {
                written += channel.write(data);
            }
            position += written;
            return written;
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    @Override
    public int read(long position, ByteBuffer data) {
        try {
            int read = 0;
            int totalRead = 0;
            while (data.hasRemaining() && read >= 0) {
                read = channel.read(data, position + totalRead);
                if (read > 0) {
                    totalRead += read;
                }
            }
            return totalRead;
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    @Override
    public long size() {
        try {
            return raf.length();
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }
}
