package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.log.Checksum;
import io.joshworks.fstore.log.ChecksumException;

import java.nio.ByteBuffer;
import java.util.Random;

public abstract class ChecksumDataReader extends DataReader {

    protected static final double DEFAULT_CHECKUM_PROB = 1;

    protected final int checksumProb;
    private final Random rand = new Random();

    public ChecksumDataReader(int maxRecordSize) {
        this(maxRecordSize, DEFAULT_CHECKUM_PROB);
    }

    public ChecksumDataReader(int maxRecordSize, double checksumProb) {
        super(maxRecordSize);
        if (checksumProb < 0 || checksumProb > 1) {
            throw new IllegalArgumentException("Checksum verification frequency must be between 0.0 and 1.0");
        }
        this.checksumProb = (int) (checksumProb * 100);
    }

    protected void checksum(int expected, ByteBuffer data, long position) {
        if (checksumProb == 0) {
            return;
        }
        if (rand.nextInt(100) < checksumProb && Checksum.crc32(data) != expected) {
            throw new ChecksumException(position);
        }
    }

}
