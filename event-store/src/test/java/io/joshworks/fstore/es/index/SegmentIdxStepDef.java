package io.joshworks.fstore.es.index;

import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import io.joshworks.fstore.core.io.DiskStorage;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SegmentIdxStepDef {


    private SegmentIndex segmentIndex;
    private SortedSet<IndexEntry> result;
    private  Optional<IndexEntry> indexEntry; //TODO move to another step def

    @Given("^a SegmentIndex with the following entries$")
    public void aSegmentIndexWithTheFollowingEntries(List<IndexEntry> indexEntries) throws IOException {
        MemIndex memIndex = new MemIndex();
        for (IndexEntry indexEntry : indexEntries) {
            memIndex.add(indexEntry.stream, indexEntry.version, indexEntry.position);
        }

        segmentIndex = SegmentIndex.write(memIndex, new DiskStorage(Files.createTempFile("segmentIndex", ".idx").toFile()));
    }

    @When("^a range of stream (\\d+), with start version (\\d+) and end version (\\d+)$")
    public void aRangeStartingOfStreamWithStartVersionAndEndVersion(long stream, int startVersion, int endVersion) {
        result = segmentIndex.range(Range.of(stream, startVersion, endVersion));
    }

    @Then("^(\\d+) entries should be returned$")
    public void entriesShouldBeReturned(int numEntries) {
        assertEquals(numEntries, result.size());
    }

    @And("^First entry must be stream (\\d+), version (\\d+)$")
    public void firstEntryMustBeStreamVersion(long stream, int version) {
        assertEquals(IndexEntry.of(stream, version), result.first());
    }

    @And("^Second entry must be stream (\\d+), version (\\d+)$")
    public void secondEntryMustBeStreamVersion(long stream, int version) {
        Iterator<IndexEntry> iterator = result.iterator();
        iterator.next();
        IndexEntry second = iterator.next();
        assertEquals(IndexEntry.of(stream, version),second);
    }


    @And("^Third entry must be stream (\\d+), version (\\d+)$")
    public void thirdEntryMustBeStreamVersion(int stream, int version) throws Throwable {
        Iterator<IndexEntry> iterator = result.iterator();
        iterator.next();
        iterator.next();
        IndexEntry second = iterator.next();
        assertEquals(IndexEntry.of(stream, version),second);
    }

    @When("^the latest version of stream (\\d+) is queried$")
    public void theLatestVersionOfStreamIsQueried(long stream) throws Throwable {
        indexEntry = segmentIndex.lastOfStream(stream);
    }



    @Then("^I should get version (\\d+) of stream (\\d+)$")
    public void iShouldGetVersionOfStream(int expectedVersion, long expectedStream) throws Throwable {
        assertTrue(indexEntry.isPresent());
        assertEquals(expectedVersion, indexEntry.get().version);
        assertEquals(expectedStream, indexEntry.get().stream);
    }
}
