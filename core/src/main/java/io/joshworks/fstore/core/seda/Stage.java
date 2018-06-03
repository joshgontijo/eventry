package io.joshworks.fstore.core.seda;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Stage<T> implements Closeable {

    private final Logger logger;

    private final SedaThreadPoolExecutor threadPool;
    private final StageHandler<T> handler;
    private final SedaContext sedaContext;
    private final String name;

    private final AtomicBoolean closed = new AtomicBoolean();

    Stage(String name,
          int corePoolSize,
          int maximumPoolSize,
          int queueSize,
          int queueHighBound,
          long keepAliveTime,
          TimeUnit unit,
          boolean blockWhenFull,
          RejectedExecutionHandler rejectionHandler,
          SedaContext sedaContext,
          StageHandler<T> handler) {
        this.logger = LoggerFactory.getLogger(name);
        this.handler = handler;
        this.name = name;
        this.sedaContext = sedaContext;
        threadPool = SedaThreadPoolExecutor.create(name, corePoolSize, maximumPoolSize, keepAliveTime, unit, queueSize, queueHighBound, rejectionHandler, blockWhenFull);
    }

    boolean submit(String correlationId, T event) {
        if (closed.get()) {
            throw new IllegalStateException("Stage is closed");
        }

        threadPool.execute(TimedRunnable.wrap(() -> {
            EventContext<T> context = new EventContext<>(correlationId, event, sedaContext);
            try {
                handler.handle(context);

            } catch (StageHandler.StageHandlerException e) {
                Throwable cause = e.getCause();
                if (cause instanceof EnqueueException) {
                    logger.error("Failed to enqueue event: {}", event);
                } else {
                    logger.error("Failed handling event: " + event, e.getCause());
                }
            } catch (Exception e) {
                logger.error("Failed handling event: " + event, e);
            }
        }));
        return true;
    }

    public StageStats stats() {
        return new StageStats(threadPool, closed.get());
    }

    public boolean closed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        logger.info("Closing stage {}", name);
        threadPool.shutdown();
    }

    public void close(long timeout, TimeUnit unit) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        close();
        try {
            logger.info("Waiting stage '{}' to terminate", name);
            threadPool.awaitTermination(timeout, unit);
            logger.info("Stage '{}' terminated", name);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public String name() {
        return name;
    }


    public static class Builder<T> {

        private final StageHandler<T> handler;

        private int corePoolSize = 1;
        private int maximumPoolSize = 1;
        private int queueSize = Integer.MAX_VALUE;
        private long keepAliveTime = 30000;
        private int queueHighBound = queueSize / 2;
        private RejectedExecutionHandler rejectionHandler;
        private boolean blockWhenFull;

        public Builder(StageHandler<T> handler) {
            this.handler = handler;
        }

        public Builder<T> corePoolSize(int corePoolSize) {
            if (maximumPoolSize <= 0) {
                throw new IllegalArgumentException("corePoolSize must be greater than zero");
            }
            this.corePoolSize = corePoolSize;
            if (corePoolSize > maximumPoolSize) {
                maximumPoolSize = corePoolSize;
            }
            return this;
        }

        public Builder<T> maximumPoolSize(int maximumPoolSize) {
            if (maximumPoolSize <= 0) {
                throw new IllegalArgumentException("maximumPoolSize must be greater than zero");
            }
            this.maximumPoolSize = maximumPoolSize;
            if (corePoolSize > maximumPoolSize) {
                corePoolSize = maximumPoolSize;
            }
            return this;
        }

        public Builder<T> queueSize(int queueSize) {
            if (maximumPoolSize <= 0) {
                throw new IllegalArgumentException("queueSize must be greater than zero");
            }
            this.queueSize = queueSize;
            return this;
        }

        public Builder<T> normalQueueOperationSize(int queueSize) {
            this.queueHighBound = queueSize;
            return this;
        }

        public Builder<T> keepAliveTime(long keepAliveTime) {
            this.keepAliveTime = keepAliveTime;
            return this;
        }

        public Builder<T> blockWhenFull() {
            this.blockWhenFull = true;
            return this;
        }

        public Builder<T> rejectionHandler(RejectedExecutionHandler rejectionHandler) {
            Objects.requireNonNull(rejectionHandler, "Rejection handler must be provided");
            this.rejectionHandler = rejectionHandler;
            return this;
        }

        Stage<T> build(String name, SedaContext sedaContext) {
            rejectionHandler = rejectionHandler == null ?  new LoggingRejectionHandler(name) : rejectionHandler;
            queueHighBound = queueHighBound > queueSize ? queueSize : queueHighBound;
            return new Stage<>(name, corePoolSize, maximumPoolSize, queueSize, queueHighBound, keepAliveTime, TimeUnit.MILLISECONDS, blockWhenFull, rejectionHandler, sedaContext, handler);
        }
    }


}
