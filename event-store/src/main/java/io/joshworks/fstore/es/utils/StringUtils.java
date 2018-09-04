package io.joshworks.fstore.es.utils;

import java.nio.charset.StandardCharsets;

public final class StringUtils {

    public static boolean isBlank(String val) {
        return val != null && val.trim().isEmpty();
    }

    public static void requireNonBlank(String val) {
        requireNonBlank(val, "Value");
    }

    public static void requireNonBlank(String val, String name) {
        if(isBlank(val)) {
            throw new IllegalArgumentException(name + " must not be null or empty");
        }
    }

    public static byte[] toUtf8Bytes(String val) {
        if(val == null) {
            return new byte[0];
        }
        return val.getBytes(StandardCharsets.UTF_8);
    }

}
