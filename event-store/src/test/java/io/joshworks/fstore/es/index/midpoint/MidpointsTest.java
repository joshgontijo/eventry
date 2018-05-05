package io.joshworks.fstore.es.index.midpoint;

import io.joshworks.fstore.es.index.IndexEntry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

public class MidpointsTest {


    private File location;
    private Midpoints midpoints;

    @Before
    public void setUp() throws Exception {
        location = Files.createTempFile(null, null).toFile();
        midpoints = new Midpoints(location, "test");
    }

    @After
    public void tearDown() throws Exception {
        Files.deleteIfExists(location.toPath());
    }

    @Test
    public void midpoint_return_the_previous_index_for_non_exact_match() {

        //given
        Midpoint m1 = new Midpoint(IndexEntry.of(1, 1, 0), 0);
        Midpoint m2 = new Midpoint(IndexEntry.of(10, 1, 0), 0);
        Midpoint m3 = new Midpoint(IndexEntry.of(100, 1, 0), 0);
        midpoints.add(m1);
        midpoints.add(m2);
        midpoints.add(m3);

        IndexEntry key = IndexEntry.of(2, 1, 0);

        //when
        int idx = midpoints.getMidpointIdx(key);

        //then
        assertEquals(0, idx);
    }

    @Test
    public void midpoint_return_the_previous_index_for_exact_match() {

        //given
        Midpoint m1 = new Midpoint(IndexEntry.of(1, 1, 0), 0);
        Midpoint m2 = new Midpoint(IndexEntry.of(10, 1, 0), 0);
        Midpoint m3 = new Midpoint(IndexEntry.of(100, 1, 0), 0);
        midpoints.add(m1);
        midpoints.add(m2);
        midpoints.add(m3);

        IndexEntry key = IndexEntry.of(1, 1, 0);

        //when
        int idx = midpoints.getMidpointIdx(key);

        //then
        assertEquals(0, idx);
    }

    @Test
    public void add() {
    }

    @Test
    public void write() {
    }

    @Test
    public void getMidpointIdx() {
    }

    @Test
    public void getMidpointFor() {
    }

    @Test
    public void delete() {
    }

    @Test
    public void inRange() {
    }

    @Test
    public void size() {
    }

    @Test
    public void first() {
    }

    @Test
    public void last() {
    }
}