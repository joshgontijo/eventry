package io.joshworks.logstore;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Read and writes <code>Page</code> to the underlying storage
 */
public interface LogStore extends Closeable {

    long append(ByteBuffer data) throws IOException;

    int read(ByteBuffer data, int offset) throws IOException;

}
