package io.joshworks.fstore.es;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.log.EventRecord;
import io.joshworks.fstore.es.log.EventLog;
import io.joshworks.fstore.es.projections.ProjectionEntry;
import io.joshworks.fstore.log.PollingSubscriber;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamPoller implements PollingSubscriber<EventRecord> {

    private final PollingSubscriber<EventRecord> logPoller;
    private final PollingSubscriber<ProjectionEntry> projectionsPoller;

    private final EventLog log;

    private final AtomicBoolean closed = new AtomicBoolean();
    //TODO duplicated
    private static final int VERIFICATION_INTERVAL_MILLIS = 500;

    public StreamPoller(PollingSubscriber<EventRecord> logPoller, PollingSubscriber<ProjectionEntry> projectionsPoller, EventLog log) {
        this.logPoller = logPoller;
        this.projectionsPoller = projectionsPoller;
        this.log = log;
    }

    private EventRecord fetchEvent(IndexEntry indexEntry) {
        return log.get(indexEntry.position);
    }

    private EventRecord tryPoll(long time, TimeUnit timeUnit) throws InterruptedException {
        EventRecord fromLog = logPoller.peek();
        ProjectionEntry fromProjection = projectionsPoller.peek();

        if (fromLog == null && fromProjection == null) {
            if (time > 0) {
                waitFor(time, timeUnit);
                fromLog = logPoller.peek();
                fromProjection = projectionsPoller.peek();
                if (fromLog == null && fromProjection == null) {
                    return null;
                }
            } else {
                return null;
            }
        }

        return getFirstEvent(fromLog, fromProjection);
    }

    private EventRecord tryTake() throws InterruptedException {
        EventRecord fromLog = logPoller.peek();
        ProjectionEntry fromProjection = projectionsPoller.peek();

        if (fromLog == null && fromProjection == null) {
            waitForData(500, TimeUnit.MILLISECONDS);
            fromLog = logPoller.peek();
            fromProjection = projectionsPoller.peek();
        }

        return getFirstEvent(fromLog, fromProjection);
    }

    private EventRecord getFirstEvent(EventRecord fromLog, ProjectionEntry fromProjection) throws InterruptedException {
        if (fromLog == null && fromProjection == null) {
            return null;
        }

        if (fromProjection == null) {
            return logPoller.poll();
        }

        if (fromLog == null) {
            ProjectionEntry polled = projectionsPoller.poll();
            return fetchEvent(polled.indexEntry);
        }

        if (fromLog.timestamp < fromProjection.timestamp) {
            return logPoller.poll();
        } else {
            ProjectionEntry poll = projectionsPoller.poll();
            return fetchEvent(poll.indexEntry);
        }
    }


    @Override
    public EventRecord peek() throws InterruptedException {
        EventRecord fromLog = logPoller.peek();
        ProjectionEntry fromProjection = projectionsPoller.peek();
        return getFirstEvent(fromLog, fromProjection);
    }

    @Override
    public synchronized EventRecord poll() throws InterruptedException {
        return tryPoll(PollingSubscriber.NO_SLEEP, TimeUnit.MILLISECONDS);
    }

    @Override
    public EventRecord poll(long limit, TimeUnit timeUnit) throws InterruptedException {
        return tryPoll(limit, timeUnit);
    }

    //FIXME duplicated
    private void waitForData(long time, TimeUnit timeUnit) throws InterruptedException {
        while (!closed.get() && !hasDataAvailable()) {
            timeUnit.sleep(time);
        }
    }

    //FIXME duplicated
    private void waitFor(long time, TimeUnit timeUnit) throws InterruptedException {
        long elapsed = 0;
        long start = System.currentTimeMillis();
        long maxWaitTime = timeUnit.toMillis(time);
        long interval = Math.min(maxWaitTime, VERIFICATION_INTERVAL_MILLIS);
        while (!closed.get() && !hasDataAvailable() && elapsed < maxWaitTime) {
            TimeUnit.MILLISECONDS.sleep(interval);
            elapsed = System.currentTimeMillis() - start;
        }
    }

    private boolean hasDataAvailable() {
        return !logPoller.headOfLog() || !projectionsPoller.headOfLog();
    }

    @Override
    public EventRecord take() throws InterruptedException {
        return tryTake();
    }

    @Override
    public boolean headOfLog() {
        return logPoller.headOfLog() && projectionsPoller.headOfLog();
    }

    @Override
    public boolean endOfLog() {
        return logPoller.endOfLog() && projectionsPoller.endOfLog();
    }

    @Override
    public long position() {
        return -1;
    }

    @Override
    public void close() {
        if(!closed.compareAndSet(false, true)) {
            return;
        }
        IOUtils.closeQuietly(logPoller);
        IOUtils.closeQuietly(projectionsPoller);
    }
}