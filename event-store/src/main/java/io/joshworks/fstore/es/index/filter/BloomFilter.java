package io.joshworks.fstore.es.index.filter;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.index.filter.Hash;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.BitSet;
import java.util.Objects;

public class BloomFilter<T> {
    private final File handler;
    private BitSet hashes;
    private Hash<T> hash;
    private int noHashes; // Number of hash functions
    private static final double LN2 = 0.6931471805599453; // ln(2)
    private boolean dirty;

    /**
     * Create a new bloom filter.
     *
     * @param numberOfBits Desired position of the container in bits
     **/
    public BloomFilter(File indexDir, String segmentFileName, int numberOfBits, Hash<T> hash) {
        this.handler = getFile(indexDir, segmentFileName);
        this.hash = hash;
        if (noHashes <= 0) noHashes = 1;
        this.hashes = handler.exists() ? load() : new BitSet(numberOfBits);
    }

    public BloomFilter(File indexDir, String segmentFileName, int elementSize, double probabilityOfFalsePositives, Hash<T> hash) {
        this.handler = getFile(indexDir, segmentFileName);
        noHashes = getNumberOfBits(probabilityOfFalsePositives, elementSize);
        if (noHashes <= 0) noHashes = 1;
        int numberOfBits = getNumberOfBits(probabilityOfFalsePositives, elementSize);
        noHashes = getOptimalNumberOfHashesByBits(elementSize, numberOfBits);
        this.hash = hash;
        this.hashes = handler.exists() ? load() : new BitSet(numberOfBits);
    }

    private static File getFile(File indexDir, String segmentName) {
        return new File(indexDir, segmentName.split("\\.")[0] + "-FILTER.dat");
    }

    /**
     * Add an element to the container
     **/
    public void add(T key) {
        for (int h : hash.hash(hashes.size(), noHashes, key))
            hashes.set(h);
        dirty = true;
    }

    /**
     * Returns true if the element is in the container.
     * Returns false with a probability ≈ 1-e^(-ln(2)² * m/n)
     * if the element is not in the container.
     **/
    public boolean contains(T key) {
        for (int h : hash.hash(hashes.size(), noHashes, key))
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
        dirty = true;
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

    public void write() {
        if(!dirty) {
            return;
        }

        byte[] bytes = hashes.toByteArray();
        try (Storage storage = new MMapStorage(handler, bytes.length, FileChannel.MapMode.READ_WRITE)) {
            storage.write(ByteBuffer.wrap(bytes));
        } catch (IOException e) {
            throw RuntimeIOException.of("Failed to write filter", e);
        }
        dirty = false;
    }

    private BitSet load() {

        try (Storage storage = new MMapStorage(handler, handler.length(), FileChannel.MapMode.READ_WRITE)) {
            ByteBuffer data = ByteBuffer.allocate((int) handler.length());
            storage.read(0, data);
            return BitSet.valueOf(data.array());
        } catch (IOException e) {
            throw RuntimeIOException.of("Failed to write filter", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BloomFilter<?> that = (BloomFilter<?>) o;
        return Objects.equals(hashes, that.hashes);
    }
}