package io.joshworks.fstore.core.util;

public abstract class Size {

    public static int BLOCK = 4096;

    public abstract long toBytes(int value);

    public static final Size BYTE = new Size() {
        @Override
        public long toBytes(int value) {
            return value;
        }
    };

    public static final Size KILOBYTE = new Size() {
        @Override
        public long toBytes(int value) {
            return value * KILOBYTE_SCALE;
        }
    };

    public static final Size MEGABYTE = new Size() {
        @Override
        public long toBytes(int value) {
            return value * MEGABYTE_SCALE;
        }
    };

    public static final Size GIGABYTE = new Size() {
        @Override
        public long toBytes(int value) {
            return value * GIGABYTE_SCALE;
        }
    };

    public static final Size TERABYTE = new Size() {
        @Override
        public long toBytes(int value) {
            return value * TERABYTE_SCALE;
        }
    };

    private static final long BYTE_SCALE = 1L;
    private static final long KILOBYTE_SCALE = BYTE_SCALE * 1024L;
    private static final long MEGABYTE_SCALE  = KILOBYTE_SCALE *  1024L;
    private static final long GIGABYTE_SCALE = MEGABYTE_SCALE * 1024L;
    private static final long TERABYTE_SCALE = GIGABYTE_SCALE * 1024L;



}
