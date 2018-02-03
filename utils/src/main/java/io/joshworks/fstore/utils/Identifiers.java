package io.joshworks.fstore.utils;

import java.util.UUID;

public class Identifiers {

    private Identifiers(){

    }

    public static String uuid() {
        return UUID.randomUUID().toString();
    }

    public static String shortUuid() {
        return uuid().substring(0,8);
    }

}
