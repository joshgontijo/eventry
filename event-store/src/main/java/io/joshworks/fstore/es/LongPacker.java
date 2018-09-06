package io.joshworks.fstore.es;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Packing utility for non-negative <code>long</code> and values.
 * <p/>
 * Originally developed for Kryo by Nathan Sweet.
 * Modified for JDBM by Jan Kotek
 */
public final class LongPacker {


    public static void main(String[] args) throws IOException {
        long original = 123456789;
        ByteBuffer bb = LongPacker.packLong(original);
        long val = LongPacker.unpackLong(bb);

        System.out.println(original);
        System.out.println(val);

    }

    /**
     * Pack  non-negative long into output stream.
     * It will occupy 1-10 bytes depending on value (lower values occupy smaller space)
     *
     * @param os
     * @param value
     * @throws IOException
     */
    static public ByteBuffer packLong(long value) throws IOException {

        if (value < 0) {
            throw new IllegalArgumentException("negative value: v=" + value);
        }

        ByteBuffer bb = ByteBuffer.allocate(20);
        while ((value & ~0x7FL) != 0) {
            bb.putInt((((int) value & 0x7F) | 0x80));
            value >>>= 7;
        }

        bb.put((byte) value);
        return bb;
//        os.write((byte) value);
    }


    /**
     * Unpack positive long value from the input stream.
     *
     * @param is The input stream.
     * @return The long value.
     * @throws java.io.IOException
     */
    static public long unpackLong(ByteBuffer bb) throws IOException {

        long result = 0;
        for (int offset = 0; offset < 64; offset += 7) {
            long b = bb.get();
            result |= (b & 0x7F) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new Error("Malformed long.");
    }


    /**
     * Pack  non-negative long into output stream.
     * It will occupy 1-5 bytes depending on value (lower values occupy smaller space)
     *
     * @param os
     * @param value
     * @throws IOException
     */

    static public void packInt(DataOutput os, int value) throws IOException {

        if (value < 0) {
            throw new IllegalArgumentException("negative value: v=" + value);
        }

        while ((value & ~0x7F) != 0) {
            os.write(((value & 0x7F) | 0x80));
            value >>>= 7;
        }

        os.write((byte) value);
    }

    static public int unpackInt(DataInput is) throws IOException {
        for (int offset = 0, result = 0; offset < 32; offset += 7) {
            int b = is.readUnsignedByte();
            result |= (b & 0x7F) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new Error("Malformed integer.");

    }

}
