package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;

public interface DataReader {

   ByteBuffer readForward(Storage storage, long position);
   ByteBuffer readBackward(Storage storage, long position);
   ByteBuffer getBuffer();

}
