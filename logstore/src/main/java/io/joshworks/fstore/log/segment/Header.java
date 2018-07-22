package io.joshworks.fstore.log.segment;

import java.util.Objects;

public class Header {

    public static final Header EMPTY = new Header(0, 0, -1, Type.EMPTY, 0);

    public static final String LOG_MAGIC = "e7297e9a-6469-11e8-adc0-fa7ae01bbebc";
    public static final int SIZE = 1024;

    public final String magic = LOG_MAGIC;
    public final long entries;
    public final long created;
    public final long size;
    public final int level;
    public final Type type;

    Header(long entries, long created, int level, Type type, long size) {
        this.entries = entries;
        this.created = created;
        this.level = level;
        this.type = type;
        this.size = size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Header header = (Header) o;
        return entries == header.entries &&
                created == header.created &&
                size == header.size &&
                level == header.level &&
                Objects.equals(magic, header.magic) &&
                type == header.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(magic, entries, created, size, level, type);
    }

    @Override
    public String toString() {
        return "Header{" + "magic='" + magic + '\'' +
                ", entries=" + entries +
                ", created=" + created +
                ", level=" + level +
                ", type=" + type +
                ", size=" + size +
                '}';
    }
}
