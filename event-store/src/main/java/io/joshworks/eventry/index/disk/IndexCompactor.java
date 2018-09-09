package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.fstore.log.appender.merge.UniqueMergeCombiner;
import io.joshworks.fstore.log.segment.Log;

import java.util.List;

public class IndexCompactor extends UniqueMergeCombiner<IndexEntry> {

    @Override
    public void merge(List<? extends Log<IndexEntry>> segments, Log<IndexEntry> output) {
//        BloomFilter
        IndexSegment indexSegment = (IndexSegment) output;
        long totalEntries = segments.stream().mapToLong(Log::entries).sum();
        indexSegment.newBloomFilter(totalEntries);
        super.merge(segments, output);

    }

}
