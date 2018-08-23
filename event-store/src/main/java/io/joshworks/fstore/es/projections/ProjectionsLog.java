package io.joshworks.fstore.es.projections;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.appenders.SimpleLogAppender;

import java.io.File;

public class ProjectionsLog {

    private static final String PROJECTIONS_DIR = "projections";
    private final SimpleLogAppender<ProjectionEntry> appender;

    public ProjectionsLog(File rootDir) {
        this.appender = new SimpleLogAppender<>(LogAppender
                .builder(new File(rootDir, PROJECTIONS_DIR), new ProjectionEntrySerializer())
                .segmentSize((int) Size.MEGABYTE.toBytes(100))
                .disableCompaction());
    }

    public void add(ProjectionEntry entry) {
        appender.append(entry);
    }

    public LogIterator<ProjectionEntry> iterator() {
        return appender.scanner();
    }

    public PollingSubscriber<ProjectionEntry> poller() {
        return appender.poller();
    }

    public PollingSubscriber<ProjectionEntry> poller(long position) {
        return appender.poller(position);
    }

    public void close() {
        appender.close();
    }

}