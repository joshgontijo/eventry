package io.joshworks.logstore;

import java.io.IOException;

/**
 * Read and writes <code>Page</code> to the underlying storage
 */
public interface LogStorage {

    long append(byte[] data) throws IOException;

    void read(byte[] data, int offset, int length) throws IOException;

    void close();
}
