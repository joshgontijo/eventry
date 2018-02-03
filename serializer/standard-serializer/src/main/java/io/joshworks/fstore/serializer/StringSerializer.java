package io.joshworks.fstore.serializer;

import io.joshworks.fstore.api.Serializer;

import java.nio.charset.StandardCharsets;

public class StringSerializer implements Serializer<String> {
//    @Override
//    public void write(String data, DataOutput out) throws IOException {
//        out.write(data.getBytes(StandardCharsets.UTF_8));
//    }
//
//    @Override
//    public void read(byte[] data, DataInput in) throws IOException {
//        in.readFully(data);
//    }

    @Override
    public byte[] toBytes(String data) {
        return data.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String fromBytes(byte[] data) {
        return fromBytes(data, 0, data.length);
    }

    @Override
    public String fromBytes(byte[] data, int offset, int length) {
        return new String(data, offset, length, StandardCharsets.UTF_8);
    }
}
