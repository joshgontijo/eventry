package io.joshworks.fstore.log.appender.naming;

import java.util.List;

public class TimestampNamingStrategy implements NamingStrategy {


    @Override
    public String name(List<String> currentSegments) {
        return String.valueOf(System.currentTimeMillis());
    }
}
