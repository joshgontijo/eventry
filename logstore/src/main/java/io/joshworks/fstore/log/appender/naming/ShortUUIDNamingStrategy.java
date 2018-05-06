package io.joshworks.fstore.log.appender.naming;

import java.util.List;
import java.util.UUID;

public class ShortUUIDNamingStrategy implements NamingStrategy {

    @Override
    public String name(List<String> currentSegments) {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
