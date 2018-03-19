package io.joshworks.fstore.mldb;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class LogEntry<T> {

    final int op; //0 - ADD, 1 - DELETE
    final T data;

    private LogEntry(int op, T data) {
        this.op = op;
        this.data = data;
    }

    public static <T> LogEntry<T> of(int op, T data) {
        return new LogEntry<T>(op, data);
    }

    public static <T> Serializer<LogEntry<T>> serializer(final Serializer<T> serializer) {
        return new Serializer<LogEntry<T>>() {
            @Override
            public ByteBuffer toBytes(LogEntry<T> entry) {
                ByteBuffer dataBuffer = serializer.toBytes(entry.data);
                ByteBuffer withOp = ByteBuffer.allocate(Integer.BYTES + dataBuffer.limit());
                return (ByteBuffer) withOp.putInt(entry.op).put(dataBuffer).flip();
            }

            @Override
            public LogEntry<T> fromBytes(ByteBuffer buffer) {
                int op = buffer.getInt();
                if(op != 0 && op != 1) {
                    throw new RuntimeException("Invalid OP");
                }
                T data = serializer.fromBytes(buffer);
                return LogEntry.of(op, data);
            }
        };
    }
}
