package io.joshworks.fstore.index;

//TODO how this is goning to be used ?
public class IndexKey {

    public final String stream;
    public final int version;

    private IndexKey(String stream, int version) {
        this.stream = stream;
        this.version = version;
    }


    public static IndexKey of(String stream, int version) {
        return new IndexKey(stream, version);
    }

    public byte[] toBytes() {
        return (stream + version).getBytes();
    }

}
