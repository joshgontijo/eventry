package io.joshworks.fstore.log.appender.it;

import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.appenders.SimpleLogAppender;
import io.joshworks.fstore.log.segment.LogSegment;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.Ignore;

import java.io.File;

@Ignore
public class MmapSimpleLogAppenderIT extends LogAppenderIT<LogSegment<String>> {

    @Override
    protected LogAppender<String, LogSegment<String>> appender(File testDirectory) {
//        return new SimpleLogAppender<>(LogAppender.builder(testDirectory, Serializers.STRING).mmap());
        return new SimpleLogAppender<>(LogAppender.builder(testDirectory, Serializers.STRING)
                .mmap(83986080)

                .segmentSize(83886080));
    }
}
