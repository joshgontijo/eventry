package io.joshworks.fstore.index.filter;

import io.joshworks.fstore.serializer.IntegerSerializer;

import java.util.BitSet;

public class BloomFilter<T> {
    private BitSet hashes;
    private Hash<T> hash;
    private int noHashes; // Number of hash functions
    private int numberOfBits; //bit position
    private static final double LN2 = 0.6931471805599453; // ln(2)

    /**
     * Create a new bloom filter.
     *
     * @param elementSize Expected number of elements
     * @param numberOfBits Desired position of the container in bits
     **/
    public BloomFilter(int elementSize, int numberOfBits, Hash<T> hash) {

        if (noHashes <= 0) noHashes = 1;
        this.numberOfBits = numberOfBits;
        this.hashes = new BitSet(numberOfBits);
        this.hash = hash;
    }

    public BloomFilter(int elementSize, double probabilityOfFalsePositives, Hash<T> hash) {
        noHashes = getNumberOfBits(probabilityOfFalsePositives, elementSize);
        if (noHashes <= 0) noHashes = 1;
        this.numberOfBits = getNumberOfBits(probabilityOfFalsePositives, elementSize);
        noHashes = getOptimalNumberOfHashesByBits(elementSize, numberOfBits);
        this.hashes = new BitSet(numberOfBits);
        this.hash = hash;
    }

//    /**
//     * Create a bloom filter of 1Mib.
//     *
//     * @param n Expected number of elements
//     **/
//    public BloomFilter(int n) {
//        this(n, 1024 * 1024 * 8);
//    }

    /**
     * Add an element to the container
     **/
    public void add(T key) {
        for (int h : hash.hash(numberOfBits, noHashes, key))
            hashes.set(h);
    }

    /**
     * Returns true if the element is in the container.
     * Returns false with a probability ≈ 1-e^(-ln(2)² * m/n)
     * if the element is not in the container.
     **/
    public boolean contains(T key) {
        for (int h : hash.hash(numberOfBits, noHashes, key))
            if (!hashes.get(h))
                return false;
        return true;
    }

    /**
     * Removes all of the elements from this filter.
     **/
    public void clear() {
        hashes.clear();
    }

    /**
     * Generate a unique hash representing the filter
     **/
    @Override
    public int hashCode() {
        return hashes.hashCode() ^ noHashes;
    }

    /**
     * Merge another bloom filter into the current one.
     * After this operation, the current bloom filter contains all elements in
     * other.
     **/
    public void merge(BloomFilter other) {
        if (other.noHashes != this.noHashes || other.hashes.size() != this.hashes.size()) {
            throw new IllegalArgumentException("Incompatible bloom filters");
        }
        this.hashes.or(other.hashes);
    }

    /**
     * noHashes = (m / n) ln 2 from wikipedia.
     *
     * @param elementSize  the number of elements expected.
     * @param numberOfBits the number of bytes allowed.
     * @return the best number of hashes.
     */
    private int getOptimalNumberOfHashesByBits(long elementSize, long numberOfBits) {
        return (int) Math.ceil(Math.log(2) * ((double) numberOfBits / elementSize));
    }


    /**
     * Calculate the number of bits needed to produce the provided probability of false
     * positives with the given element position.
     *
     * @param probabilityOfFalsePositives the probability of false positives.
     * @param elementSize                 the estimated element position.
     * @return the number of bytes.
     */
    private static int getNumberOfBits(double probabilityOfFalsePositives, long elementSize) {
        return (int) (Math.abs(elementSize * Math.log(probabilityOfFalsePositives)) / (Math.pow(Math.log(2), 2)));
    }

    public static void main(String[] args) {
        BloomFilter<Integer> filter = new BloomFilter<>(10, 50, Hash.Murmur64(new IntegerSerializer()));
        filter.add(1);
        filter.add(2);
        filter.add(3);

        System.out.println(filter.contains(1));
        System.out.println(filter.contains(2));
        System.out.println(filter.contains(3));
        System.out.println(filter.contains(3));
        System.out.println(filter.contains(4));
        System.out.println(filter.contains(5));
    }

}