package io.joshworks.fstore.core.io;

import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class RafBaseStorageTest extends DiskStorageTest {

    @Override
    protected Storage store(File file, long size) {
        return new RafStorage(file, size, Mode.READ_WRITE);
    }

    @Test
    public void shrink_resize_the_file_to_the_position() {
        ByteBuffer bb = ByteBuffer.wrap(TEST_DATA.getBytes(StandardCharsets.UTF_8));
        storage.write(bb);

        long pos = storage.position();
        storage.truncate(pos);

        assertEquals(pos, storage.length());
    }

}