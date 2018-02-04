package io.joshworks.fstore.log;

import io.joshworks.fstore.api.Serializer;

import java.nio.file.Path;

public class LogAppender<T> implements Writer<T> {


    private final Path directory;
    private final Serializer<T> serializer;
    private final int segmentSize;

//    private final
//
//    public static <T> LogAppender<T> create(Path directory, String name, Serializer<T> serializer, long fileSize) {
//
//
//    }

    private LogAppender(Path directory, Serializer<T> serializer, int segmentSize) {
        this.directory = directory;
        this.serializer = serializer;
        this.segmentSize = segmentSize;
    }

    @Override
    public long write(T data) {
        return 0;
    }
}
