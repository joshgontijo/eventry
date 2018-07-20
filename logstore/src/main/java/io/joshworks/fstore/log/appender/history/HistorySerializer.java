package io.joshworks.fstore.log.appender.history;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class HistorySerializer implements Serializer<HistoryItem> {
    @Override
    public ByteBuffer toBytes(HistoryItem data) {
        return null;
    }

    @Override
    public void writeTo(HistoryItem data, ByteBuffer dest) {

    }

    @Override
    public HistoryItem fromBytes(ByteBuffer buffer) {
        return null;
    }
}
