package io.joshworks.fstore.log.appender.it;

import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.appenders.SimpleLogAppender;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.Ignore;

import java.io.File;

@Ignore
public class MmapSimpleLogAppenderIT extends LogAppenderIT<Segment<String>> {

    @Override
    protected LogAppender<String, Segment<String>> appender(File testDirectory) {
//        return new SimpleLogAppender<>(LogAppender.builder(testDirectory, Serializers.STRING).mmap());
        return new SimpleLogAppender<>(LogAppender.builder(testDirectory, Serializers.STRING)
                .mmap(83986080)
                .segmentSize(83886080));
    }
}
