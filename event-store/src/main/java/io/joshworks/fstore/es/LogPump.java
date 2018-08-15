package io.joshworks.fstore.es.subscription;

import io.joshworks.fstore.log.LogIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

//Pull based log reader
public class LogPump<T> implements Runnable, Closeable {

    private final long pollInterval;
    private int maxBatchSize;
    private AtomicBoolean stop = new AtomicBoolean();
    private LogIterator<T> reader;
    private long processed;

    private final Logger logger = LoggerFactory.getLogger(LogPump.class);

    public LogPump(long pollInterval, int maxBatchSize, LogIterator<T> reader) {
        this.pollInterval = pollInterval;
        this.maxBatchSize = maxBatchSize;
        this.reader = reader;
    }

    @Override
    public void run() {
        logger.info("Starting log chaser");
        while (!stop.get()) {
            List<T> data = new ArrayList<>(maxBatchSize);
            while (reader.hasNext()) {
                data.add(reader.next());
                if (data.size() >= maxBatchSize) {
                    process(data);
                    data = new ArrayList<>();
                }
            }
            if (!data.isEmpty()) {
                process(data);
            }
            sleep();
        }
        logger.info("Log chaser stopped");
    }

    private void sleep() {
        try {
            TimeUnit.MILLISECONDS.sleep(pollInterval);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }


    private void process(List<T> data) {
        int size = data.size();
        logger.info("Broadcasting {} entries: ", size);
        processed += size;
        //sync / blocking operation
        data.forEach(System.out::println);
    }

    public long position() {
        return reader.position();
    }

    @Override
    public void close() {
        logger.info("Log chaser stop request");
        stop.set(true);
    }
}