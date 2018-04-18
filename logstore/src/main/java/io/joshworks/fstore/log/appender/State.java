package io.joshworks.fstore.log.appender;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class State {

    long position;
    long entryCount;
    final List<String> segments = new LinkedList<>();

    public State(long position, long entryCount, String[] segments) {
        this.position = position;
        this.entryCount = entryCount;
        this.segments.addAll(Arrays.asList(segments));
    }

    public static State readFrom(DataInput in) throws IOException {
        long lastPosition = in.readLong();
        long entryCount = in.readLong();
        String segments = in.readUTF();

        return new State(lastPosition, entryCount, segments.split(","));
    }

    public static State empty() {
        return new State(0L,0L, new String[]{});
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeLong(position);
        out.writeLong(entryCount);
        out.writeUTF(String.join(",", segments));
    }


    @Override
    public String toString() {
        return "State{" + "position=" + position +
                ", entryCount=" + entryCount +
                ", segments=" + segments +
                '}';
    }
}
