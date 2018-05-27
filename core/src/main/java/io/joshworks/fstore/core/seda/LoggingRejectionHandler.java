package io.joshworks.fstore.core.seda;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

class LoggingRejectionHandler implements RejectedExecutionHandler {

    private static final Logger logger = LoggerFactory.getLogger(LoggingRejectionHandler.class);

    private final String name;

    LoggingRejectionHandler(String name) {
        this.name = name;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        logger.warn("[{}] Task rejected", name);
    }
}
