package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.log.Utils;
import io.joshworks.fstore.log.segment.block.BlockSerializer;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;

import java.io.File;

public class BlockAppenderIT {

    private BlockAppender<String> appender;
    private File testDirectory;
    private static final int FLUSH_THRESHOLD = 1000000;
    private static final boolean USE_COMPRESSION = true;

    @Before
    public void setUp() {
        testDirectory = Utils.testFolder();
        appender = new BlockAppender<>(new Config<>(testDirectory, new BlockSerializer<>(Serializers.STRING, Codec.noCompression())), Serializers.STRING, 4096);
    }

    @After
    public void tearDown() {
        appender.close();
        Utils.tryDelete(new File(testDirectory, "index"));
        Utils.tryDelete(testDirectory);
    }

//    @Test
//    public void poll_returns_data_from_disk_and_memory_IT() throws InterruptedException {
//        int totalEntries = 5000000;
//
//        new Thread(() -> {
//            for (int i = 0; i < totalEntries; i++) {
//                appender.add(i, 0, 0);
//            }
//        }).start();
//
//        try(PollingSubscriber<Block<String>> poller = appender.poller()) {
//            int entries =
//            while()
//            for (int i = 0; i < totalEntries; i++) {
//                Block<String> poll = poller.poll();
////                System.out.println(poll);
//                assertEquals("Failed on " + i + ": " + poll, String.valueOf(i), poll);
//            }
//
//        }
//    }


}