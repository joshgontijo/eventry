package io.joshworks.fstore.log.appender.it;

import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.appenders.SimpleLogAppender;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;

public class RafLogAppenderIT extends LogAppenderIT<Segment<String>> {

    @Override
    protected LogAppender<String, Segment<String>> appender(File testDirectory) {
        return new SimpleLogAppender<>(LogAppender.builder(testDirectory, Serializers.STRING)
                .segmentSize(83886080)
                .threadPerLevelCompaction()
                .disableCompaction());
    }
}
