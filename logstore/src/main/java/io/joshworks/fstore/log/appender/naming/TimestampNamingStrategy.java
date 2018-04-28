package io.joshworks.fstore.log.appender.naming;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TimestampNamingStrategy implements NamingStrategy {

    @Override
    public String name(List<String> currentSegments) {
        Set<String> names = new HashSet<>();
        for (String currentSegment : currentSegments) {
            names.add(currentSegment.split("\\.")[0]);
        }
        String ts;
        do {
            ts = String.valueOf(System.currentTimeMillis());
        } while (names.contains(ts));

        return ts;
    }
}
