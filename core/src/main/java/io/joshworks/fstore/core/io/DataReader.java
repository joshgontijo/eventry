package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;

public interface DataReader {

   ByteBuffer read(Storage storage, long position);

}
