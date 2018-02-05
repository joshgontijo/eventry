package io.joshworks.fstore.utils.io.bp;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class BufferPool {

    private static final BlockingQueue<ByteBuffer> pool = new LinkedBlockingQueue<>();

    static {
        for (int i = 0; i < 100; i++) {

        }
    }

    private BufferPool() {

    }

    public static void fromBuffer(Consumer<ByteBuffer> consumer) {
        try {
            ByteBuffer bb = pool.take();
            consumer.accept(bb);
            bb.clear();
            pool.add(bb);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


}
