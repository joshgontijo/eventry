package io.joshworks.fstore.log.segment;

public class Marker {

    public final long logStart;
    public final long head;
    public final long footerStart;
    public final long footerEnd;


    Marker(long logStart, long head, long footerStart, long footerEnd) {
        this.logStart = logStart;
        this.head = head;
        this.footerStart = footerStart;
        this.footerEnd = footerEnd;
    }
}
