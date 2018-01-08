package io.joshworks.fstore.page;

import io.joshworks.fstore.Constants;

public class PageManager {

    private final int pageSize;

    public PageManager(int pageSize) {
        this.pageSize = pageSize;
    }

    public static Page allocatePage() {
        return null;
    }

    public static long locationOf(Page page) {
        return page.id() * Constants.PAGE_SIZE;
    }

}
