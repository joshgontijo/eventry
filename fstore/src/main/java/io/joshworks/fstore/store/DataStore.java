package io.joshworks.fstore.store;

import io.joshworks.fstore.page.Page;

/**
 * Read and writes <code>Page</code> to the underlying storage
 */
public interface DataStore {

    void write(Page page);

    Page read(int pageId);

    void close();
}
