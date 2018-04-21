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
    long lastRollTime;
    String currentSegment;
    List<String> rolledSegments;

    private State(long position, long entryCount, long lastRollTime, String currentSegment, String[] rolledSegments) {
        this.position = position;
        this.entryCount = entryCount;
        this.lastRollTime = lastRollTime;
        this.currentSegment = currentSegment == null || currentSegment.isEmpty() ? null : currentSegment;
        this.rolledSegments = new LinkedList<>(Arrays.asList(rolledSegments));
    }

    public static State readFrom(DataInput in) throws IOException {
        long lastPosition = in.readLong();
        long entryCount = in.readLong();
        long lastRollTime = in.readLong();
        String currentSegment = in.readUTF();
        String segments = in.readUTF();

        return new State(lastPosition, entryCount, lastRollTime, currentSegment, segments.isEmpty() ? new String[0] : segments.split(","));
    }

    public static State empty() {
        return new State(0L, 0L, System.currentTimeMillis(), null, new String[]{});
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeLong(position);
        out.writeLong(entryCount);
        out.writeLong(lastRollTime);
        out.writeUTF(currentSegment == null ? "" : currentSegment);
        out.writeUTF(String.join(",", rolledSegments));
    }

    @Override
    public String toString() {
        String sb = "State{" + "position=" + position +
                ", entryCount=" + entryCount +
                ", lastRollTime=" + lastRollTime +
                ", currentSegment='" + currentSegment + '\'' +
                ", rolledSegments=" + rolledSegments +
                '}';
        return sb;
    }
}
