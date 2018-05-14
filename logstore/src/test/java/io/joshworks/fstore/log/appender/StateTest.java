package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class StateTest {

    private File testFile;
    private State state;

    @Before
    public void setUp() {
        testFile = Utils.testFolder();
        state = State.empty(testFile);
    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(state);
        Utils.tryDelete(testFile);
    }

    @Test
    public void flush() {

        //give
        state.position(10);
        state.incrementEntryCount();
        state.lastRollTime(123L);

        List<List<String>> segments = new ArrayList<>();
        segments.add(Arrays.asList("a", "b"));
        state.levels(segments);

        state.close();

        //when
        try(State found = State.readFrom(testFile)) {
            //then
            assertEquals(state.position(), found.position());
            assertEquals(state.entryCount(), found.entryCount());

        }


    }
}