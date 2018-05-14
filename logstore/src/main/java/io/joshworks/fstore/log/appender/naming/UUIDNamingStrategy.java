package io.joshworks.fstore.log.appender.naming;

import java.util.UUID;

public class UUIDNamingStrategy implements NamingStrategy {

    @Override
    public String prefix() {
        return UUID.randomUUID().toString();
    }
}
