package io.joshworks.fstore;

public class Constants {

    public static final int PAGE_SIZE = 4096; //4k
    public static final int FILE_PAGE_SIZE = 1073741824; //1GB

    //TODO add extent ?
    public static final int PAGE_PER_FILE = FILE_PAGE_SIZE / PAGE_SIZE;
}
