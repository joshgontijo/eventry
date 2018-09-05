package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

public class PooledDataReader extends ChecksumDataReader {

    private final BlockingQueue<DataReader> readers;
    private static final int DEFAULT_READER_SIZE = 1000;

    public PooledDataReader(Supplier<DataReader> readerSupplier) {
        this(DEFAULT_READER_SIZE, DEFAULT_CHECKUM_PROB, readerSupplier);
    }

    public PooledDataReader(int maxReaders, Supplier<DataReader> readerSupplier) {
       this(maxReaders, DEFAULT_CHECKUM_PROB, readerSupplier);
    }

    public PooledDataReader(int maxReaders, double checksumProb, Supplier<DataReader> readerSupplier) {
        super(checksumProb);
        this.readers = new ArrayBlockingQueue<>(maxReaders);
        for (int i = 0; i < maxReaders; i++) {
            this.readers.offer(readerSupplier.get());
        }
    }

    @Override
    public ByteBuffer readForward(Storage storage, long position) {
        return read(storage, position);
    }

    private ByteBuffer read(Storage storage, long position) {
        DataReader reader = null;
        try {
            reader = readers.take();
            return reader.readForward(storage, position);
        } catch (InterruptedException e) {
            throw new RuntimeException("Could not acquire reader", e);
        } finally {
            if (reader != null)
                readers.offer(reader);
        }
    }

    @Override
    public ByteBuffer readBackward(Storage storage, long position) {
        return read(storage, position);
    }

    @Override
    public ByteBuffer getBuffer() {
        throw new UnsupportedOperationException();
    }
}
