package io.joshworks.fstore.index.filter;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.Random;

public interface Hash<T> {


    int[] hash(int maximum, int k, T val);

    static <T> Hash<T> java() {
        return new JavaHash<>();
    }

    static <T> Hash<T> Murmur64(Serializer<T> serializer) {
        return new Murmur64<>(serializer);
    }

    class Murmur64<T> implements Hash<T> {

        private static final long SEED = 0x4F7DFD47E49L;
        private final Serializer<T> serializer;

        public Murmur64(Serializer<T> serializer) {
            this.serializer = serializer;
        }


        @Override
        public int[] hash(int m, int k, T val) {
            ByteBuffer bb = serializer.toBytes(val);
            if(!bb.hasArray()) {
                throw new IllegalStateException("ByteBuffer must be backed by array");
            }
            long bitSize = m;
//            Hashing.murmur3_128().hashObject(object, funnel).asLong();
            long hash64 = MurmurHash3.MurmurHash3_x64_64(bb.array(), SEED);
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            boolean bitsChanged = false;
            int[] result = new int[k];
            for (int i = 1; i <= k; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                result[i - 1] = (int) (combinedHash % bitSize);
//                bitsChanged |= bits.set(combinedHash % bitSize);
            }
            return result;
        }
    }

    class JavaHash<T> implements Hash<T> {

        private final Random rand = new Random();

        @Override
        public int[] hash(int max, int k, T val) {
            if(val == null) {
                throw new IllegalArgumentException("Value must not be null");
            }
            rand.setSeed(val.hashCode());
            int[] hashes = new int[k];
            for (int i = 0; i < hashes.length; i++) {
                int hash = rand.nextInt() % max;
                hashes[i] = hash < 0 ? ~hash : hash;
            }
            return hashes;
        }
    }


}
