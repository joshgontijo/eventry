package io.joshworks.fstore.log.reader;

import java.nio.ByteBuffer;

//NOT THREAD SAFE
public class SingletonBufferDataReader extends FixedBufferDataReader {

    private ByteBuffer buffer;

    @Override
    public ByteBuffer getBuffer() {
        if(buffer == null) {
            this.buffer = super.getBuffer();
        }
        ByteBuffer buffer = super.getBuffer();
        buffer.clear();
        return buffer;
    }
}
