package io.joshworks.eventry.index.filter;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.BitSet;
import java.util.Objects;

public class BloomFilter<T> {
    private final File handler;

    BitSet hashes;
    private BloomFilterHasher<T> hash;
    private final int m; // The number of bits in the filter
    private int k; // Number of hash functions

    private boolean dirty;

    private static final Logger logger = LoggerFactory.getLogger(BloomFilter.class);

    /**
     * @param handler The target file
     * @param n       The expected number of elements in the filter
     * @param p       The acceptable false positive rate
     * @param hash    The hash implementation
     */
    private BloomFilter(File handler, long n, double p, BloomFilterHasher<T> hash) {
        Objects.requireNonNull(handler, "Handler");
        Objects.requireNonNull(hash, "Hash");

        this.handler = handler;
        this.m = getNumberOfBits(p, n);
        this.k = getOptimalNumberOfHashesByBits(n, this.m);
        this.hash = hash;
        this.hashes = new BitSet(this.m);
    }

    /**
     * Used to load from file only
     *
     * @param handler The file handler of this filter
     * @param hashes  The table containing the data
     * @param hash    The hash implementation (must remain the same)
     * @param m       The number of bits in the 'hashes'
     * @param k       The number of hash functions
     */
    private BloomFilter(File handler, BitSet hashes, BloomFilterHasher<T> hash, int m, int k) {
        this.handler = handler;
        this.hashes = hashes;
        this.hash = hash;
        this.m = m;
        this.k = k;
    }

    public static <T> BloomFilter<T> openOrCreate(File indexDir, String segmentFileName, long n, double p, BloomFilterHasher<T> hash) {
        File handler = getFile(indexDir, segmentFileName);
        if (handler.exists()) {
            return load(handler, hash);
        }
        return new BloomFilter<>(handler, n, p, hash);
    }

    private static File getFile(File indexDir, String segmentName) {
        return new File(indexDir, segmentName.split("\\.")[0] + ".ftr");
    }

    public void delete() {
        try {
            Files.delete(handler.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Add an element to the container
     */
    public void add(T key) {
        for (int h : hash.hash(hashes.size(), k, key))
            hashes.set(h);
        dirty = true;
    }

    /**
     * Returns true if the element is in the container.
     * Returns false with a probability ≈ 1-e^(-ln(2)² * m/n)
     * if the element is not in the container.
     **/
    public boolean contains(T key) {
        for (int h : hash.hash(hashes.size(), k, key))
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
        return hashes.hashCode() ^ k;
    }

    /**
     * Merge another bloom filter into the current one.
     * After this operation, the current bloom filter contains all elements in
     * other.
     **/
    public void merge(BloomFilter other) {
        if (other.k != this.k || other.hashes.size() != this.hashes.size()) {
            throw new IllegalArgumentException("Incompatible bloom filters");
        }
        this.hashes.or(other.hashes);
        dirty = true;
    }

    /**
     * k = (m / n) ln 2 from wikipedia.
     *
     * @param n the number of elements expected.
     * @param m the number of bytes allowed.
     * @return the best number of hashes.
     */
    private int getOptimalNumberOfHashesByBits(long n, long m) {
        return (int) Math.ceil(Math.log(2) * ((double) m / n));
    }


    /**
     * Calculate the number of bits needed to produce the provided probability of false
     * positives with the given element position.
     *
     * @param p The probability of false positives.
     * @param n The estimated number of elements.
     * @return The number of bits.
     */
    private static int getNumberOfBits(double p, long n) {
        return (int) (Math.abs(n * Math.log(p)) / (Math.pow(Math.log(2), 2)));
    }

    private static int HEADER_SIZE = Integer.BYTES * 3;
    public synchronized void write() {
        if (!dirty) {
            return;
        }

        long[] items = hashes.toLongArray();
        int dataLength = items.length * Long.BYTES;
        int totalSize = dataLength + HEADER_SIZE;
        try (Storage storage = new RafStorage(handler, totalSize, Mode.READ_WRITE)) {

            //Format
            //Length -> 4bytes
            //Number of bits (m) -> 4bytes
            //Number of hash (k) -> 4bytes
            //Data -> long[]
            ByteBuffer bb = ByteBuffer.allocate(totalSize);
            bb.putInt(dataLength);
            bb.putInt(this.m);
            bb.putInt(this.k);
            for (long item : items) {
                bb.putLong(item);
            }

            bb.flip();
            storage.write(bb);
            dirty = false;

        } catch (IOException e) {
            throw RuntimeIOException.of("Failed to write filter", e);
        }
    }

    private static <T> BloomFilter<T> load(File handler, BloomFilterHasher<T> hash) {
        try (Storage storage = new RafStorage(handler, handler.length(), Mode.READ_WRITE)) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            storage.read(0, header);
            header.flip();

            int length = header.getInt();
            int m = header.getInt();
            int k = header.getInt();

            ByteBuffer data = ByteBuffer.allocate(length);
            storage.read(HEADER_SIZE, data);

            data.flip();


            long[] longs = new long[data.remaining() / Long.BYTES];
            int i = 0;
            while(data.hasRemaining()) {
                longs[i++]= data.getLong();
            }

            BitSet bitSet = new BitSet(m);
            bitSet.or(BitSet.valueOf(longs));

            return new BloomFilter<>(handler, bitSet, hash, m, k);

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