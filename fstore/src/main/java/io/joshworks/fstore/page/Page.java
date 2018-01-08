package io.joshworks.fstore.page;

import java.nio.ByteBuffer;

/**
 * Page (or Block) is the smallest unit of a table. Every page contains a header and a data section
 * The header stores information about this page, such as pageId, free space and page type.
 * The data section contains zero or many <code>Row</code>
 */
public class Page {

    //id is the page location in the file
    private final int id;
    private final ByteBuffer data;

    Page(int id, ByteBuffer data) {
        this.id = id;
        this.data = data;
    }

    public static Page allocate(int id, int pageSize) {
        return new Page(id, ByteBuffer.allocate(pageSize));
    }

    public static Page load(int id) {
       throw new UnsupportedOperationException("NOT IMPLEMENTED");
    }

    public int id() {
        return id;
    }

}
