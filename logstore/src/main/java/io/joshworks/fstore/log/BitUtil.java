package io.joshworks.fstore.log;

import java.math.BigInteger;
import java.util.BitSet;

public class BitUtil {

    private BitUtil() {

    }

    public static long maxValueForBits(int numBits) {
        return (long) Math.pow(2, numBits) - 1;
    }

    public static long maxBitsForNumber(long number) {
        return (long) (Math.log(number) / Math.log(2)) + 1;
    }

    /**
     * @param bits the bitset to convert
     * @return Returns a byte array of at least length 1. The most significant bit in the result is guaranteed not to be
     * a 1 (since BitSet does not support sign extension). The byte-ordering of the result is big-endian which
     * means the most significant bit is in element 0. The bit at index 0 of the bit set is assumed to be the
     * least significant bit.
     */
    public static byte[] toByteArray(final BitSet bits) {
        byte[] bytes = new byte[bits.length() / 8 + 1];
        for (int i = 0; i < bits.length(); i++) {
            if (bits.get(i)) {
                bytes[bytes.length - i / 8 - 1] |= 1 << (i % 8);
            }
        }
        return bytes;
    }

    /**
     * @param bits   the bitset to convert
     * @param length the length of the set
     * @return Returns an int. The most significant bit in the result is guaranteed not to be a 1 (since BitSet does not
     * support sign extension). The int-ordering of the result is big-endian which means the most significant
     * bit is in element 0. The bit at index 0 of the bit set is assumed to be the least significant bit.
     */
    public static int toInt(final BitSet bits, final int length) {
        byte[] bytes = BitUtil.toByteArray(bits);
        int value = new BigInteger(bytes).intValue();
        if (value > Math.pow(2, length - 1) && length != -1) {
            value = value - (int) Math.pow(2, length);
        }
        return value;
    }

    /**
     * constructs a new BitSet from a string in the "110110" format.
     *
     * @param value the value
     * @return the BitSet
     */
    public static BitSet fromString(String value) {
        if (!value.startsWith("{")) {
            BitSet set = new BitSet(value.length());
            for (int i = 0; i < value.length(); i++) {
                if (value.charAt(i) == '1') {
                    set.set(i, true);
                } else if (value.charAt(i) == '0') {
                    set.set(i, false);
                } else {
                    throw new IllegalArgumentException("value should only contain ones and zeros. Try 110011");
                }
            }
            return set;
        }
        BitSet set = new BitSet();
        value = value.substring(1, value.length() - 1);
        if (value.equals("")) {
            return set;
        }
        String[] bits = value.split(",");
        for (int i = 0; i < bits.length; i++) {
            bits[i] = bits[i].trim();
            set.set(Integer.valueOf(bits[i]).intValue());
        }
        return set;
    }

    /**
     * @param bytes the byteArray
     * @return Returns a bitset containing the values in bytes.The byte-ordering of bytes must be big-endian which means
     * the most significant bit is in element 0.
     */
    public static BitSet fromByteArray(final byte[] bytes) {
        BitSet bits = new BitSet();
        for (int i = 0; i < bytes.length * 8; i++) {
            if ((bytes[bytes.length - i / 8 - 1] & (1 << (i % 8))) > 0) {
                bits.set(i);
            }
        }
        return bits;
    }

    /**
     * returns the bitset of an integer value.
     *
     * @param value  the value
     * @param length the length of the bitSet to produce
     * @return the BitSet
     */
    public static BitSet fromInt(final int value, final int length) {
        return BitUtil.fromInteger(new Integer(value), length);
    }

    /**
     * returns the bitset of an integer value.
     *
     * @param value  the value
     * @param length the length of
     * @return the BitSet
     */
    public static BitSet fromInteger(Integer value, final int length) {
        if (value.intValue() < 0 && length != -1) {
            value = new Integer((int) Math.pow(2, length) + value.intValue());
        }
        return BitUtil.fromByteArray(new BigInteger(value.toString()).toByteArray());
    }

    /**
     * returns a one-size BitSet with value.
     *
     * @param value the value of the bitSet
     * @return the BitSet
     */
    public static BitSet fromBoolean(final boolean value) {
        BitSet result = new BitSet(1);
        result.set(0, value);
        return result;
    }

}
