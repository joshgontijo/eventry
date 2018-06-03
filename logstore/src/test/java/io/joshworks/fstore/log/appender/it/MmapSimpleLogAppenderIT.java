package io.joshworks.fstore.log.appender.it;

import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.segment.LogSegment;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;

public class MmapSimpleLogAppenderIT extends LogAppenderIT<LogSegment<String>> {

    @Override
    protected LogAppender<String, LogSegment<String>> appender(File testDirectory) {
        return LogAppender.builder(testDirectory, Serializers.STRING).mmap().simple();
    }
}
