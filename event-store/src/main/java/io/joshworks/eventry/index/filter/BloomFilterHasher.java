package io.joshworks.eventry.index.filter;

import io.joshworks.eventry.hash.Murmur3;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public interface BloomFilterHasher<T> {


    int[] hash(int maximum, int k, T val);

    static <T> BloomFilterHasher<T> Murmur64(Serializer<T> serializer) {
        return new Murmur64<>(serializer);
    }

    class Murmur64<T> implements BloomFilterHasher<T> {

        private final Serializer<T> serializer;

        private Murmur64(Serializer<T> serializer) {
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
            long hash64 = Murmur3.hash64(bb.array());
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


}
