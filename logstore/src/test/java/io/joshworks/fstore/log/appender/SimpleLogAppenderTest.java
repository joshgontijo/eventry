package io.joshworks.fstore.log.appender;

public class SimpleLogAppenderTest extends LogAppenderTest {

    @Override
    protected LogAppender<String> appender(Builder<String> builder) {
        return LogAppender.simpleLog(builder);
    }
}
