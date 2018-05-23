package io.joshworks.fstore.core.seda;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Stage<T> implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Stage.class);

    private final ThreadPoolExecutor executor;
    private final StageHandler<T> handler;
    private final LocalSedaContext sedaContext;
    private final String name;

    Stage(String name,
          int corePoolSize,
          int maximumPoolSize,
          int queueSize,
          long keepAliveTime,
          TimeUnit unit,
          RejectedExecutionHandler rejectionHandler,
          LocalSedaContext sedaContext,
          StageHandler<T> handler) {
        this.handler = handler;
        this.name = name;
        this.sedaContext = sedaContext;
        executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, new LinkedBlockingQueue<>(queueSize), new StageThreadFactory(name), rejectionHandler);
    }

    void submit(String correlationId, T event) {
        executor.submit(() -> {
            try {
                handler.accept(new EventContext<>(correlationId, event, sedaContext));
            } catch (StageHandler.StageHandlerException e) {
                logger.error("[" + name + "] Failed handling event" + event, e.getCause());
            } catch (Exception e) {
                logger.error("[" + name + "] Failed handling event" + event, e);
            }
        });
    }

    @Override
    public void close() {
        logger.info("Closing stage {}", name);
        executor.shutdown();
    }


    public static class Builder<T> {

        private final String name;
        private final StageHandler<T> handler;

        private int corePoolSize = 1;
        private int maximumPoolSize = 10;
        private int queueSize = 1000;
        private long keepAliveTime = 30000;
        private double higherBound = 0.8;
        private double lowerBound = 0.8;
        private RejectedExecutionHandler rejectionHandler;

        public Builder(String name, StageHandler<T> handler) {
            this.name = name;
            this.rejectionHandler = new LoggingRejectionHandler(name);
            this.handler = handler;
        }

        public Builder<T> corePoolSize(final int corePoolSize) {
            this.corePoolSize = corePoolSize;
            return this;
        }

        public Builder<T> maximumPoolSize(final int maximumPoolSize) {
            this.maximumPoolSize = maximumPoolSize;
            return this;
        }

        public Builder<T> queueSize(final int queueSize) {
            this.queueSize = queueSize;
            return this;
        }

        public Builder<T> keepAliveTime(final long keepAliveTime) {
            this.keepAliveTime = keepAliveTime;
            return this;
        }

        public Builder<T> rejectionHandler(final RejectedExecutionHandler rejectionHandler) {
            this.rejectionHandler = rejectionHandler;
            return this;
        }

        public Builder<T> lowerBound(double lowerBound) {
            if(lowerBound > 1 && lowerBound < 0) {
                throw new IllegalArgumentException("Value must be between 0 and 1");
            }
            this.lowerBound = lowerBound;
            return this;
        }

        public Builder<T> higherBound(double higherBound) {
            if(higherBound > 1 && higherBound <= 0) {
                throw new IllegalArgumentException("Value must be between 0 (exclusive) and 1");
            }
            this.higherBound = higherBound;
            return this;
        }

        Stage<T> build(LocalSedaContext sedaContext) {
            return new Stage<>(name, corePoolSize, maximumPoolSize, queueSize, keepAliveTime, TimeUnit.MILLISECONDS, rejectionHandler, sedaContext, handler);
        }
    }

    private static class LoggingRejectionHandler implements RejectedExecutionHandler {

        private static final Logger logger = LoggerFactory.getLogger(LoggingRejectionHandler.class);

        private final String name;

        private LoggingRejectionHandler(String name) {
            this.name = name;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            logger.warn("[" + name + "] Task rejected");
        }
    }

    private static class StageThreadFactory implements ThreadFactory {

        private final String name;

        private StageThreadFactory(String name) {
            this.name = name;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName(name);
            return thread;
        }
    }


}
