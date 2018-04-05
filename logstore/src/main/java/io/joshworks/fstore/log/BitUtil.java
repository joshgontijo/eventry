package io.joshworks.fstore.log;

public class BitUtil {

    private BitUtil() {

    }

    public static long maxValueForBits(int numBits) {
        return (long) Math.pow(2, numBits) - 1;
    }

}
