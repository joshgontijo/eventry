package io.joshworks.fstore.log.segment;

public class SegmentState {

    public final int entries;
    public final long position;

    public SegmentState(int entries, long position) {
        this.entries = entries;
        this.position = position;
    }
}
