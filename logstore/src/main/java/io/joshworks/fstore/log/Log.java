package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

public interface Log<T> extends Writer<T>, Closeable {

    Logger logger = LoggerFactory.getLogger(Log.class);

    int HEADER_SIZE = Integer.BYTES * 2; //length + crc32

    String name();

    Scanner<T> scanner();

    Stream<T> stream();

    Scanner<T> scanner(long position);

    long position();

    T get(long position);

    T get(long position, int length);

    long size();

    static long write(Storage storage, ByteBuffer bytes) {
        ByteBuffer bb = ByteBuffer.allocate(HEADER_SIZE + bytes.remaining());
        bb.putInt(bytes.remaining());
        bb.putInt(Checksum.crc32(bytes));
        bb.put(bytes);

        bb.flip();
        return storage.write(bb);
    }

    static <T> long checkIntegrity(long lastKnownPosition, Log<T> log) {
        long position = lastKnownPosition;
        try {
            logger.info("Restoring log state and checking consistency from position {}", lastKnownPosition);
            Scanner<T> scanner = log.scanner(lastKnownPosition);
            while (scanner.hasNext()) {
                T next = scanner.next();
                if (next == null) {
                    logger.warn("Found inconsistent entry on position {}, segment '{}'", position, log.name());
                    break;
                }
                position = scanner.position();
            }
        } catch (Exception e) {
            logger.warn("Found inconsistent entry on position {}, segment '{}'", position, log.name());
            return position;
        }
        logger.info("Log state restored, current position {}", position);
        return position;
    }
}
