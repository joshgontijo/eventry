package io.joshworks.fstore.log.reader;

import java.nio.ByteBuffer;

//NOT THREAD SAFE
public class SingletonBufferDataReader extends FixedBufferDataReader {

    private ByteBuffer buffer;

    public SingletonBufferDataReader(int maxRecordSize) {
        super(maxRecordSize);
    }

    @Override
    public ByteBuffer getBuffer() {
        if(buffer == null) {
            this.buffer = super.getBuffer();
        }
        ByteBuffer bb = super.getBuffer();
        bb.clear();
        return bb;
    }
}
