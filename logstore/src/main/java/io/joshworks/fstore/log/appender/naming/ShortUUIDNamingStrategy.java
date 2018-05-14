package io.joshworks.fstore.log.appender.naming;

import java.util.UUID;

public class ShortUUIDNamingStrategy implements NamingStrategy {

    @Override
    public String prefix() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
