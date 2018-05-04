//package io.joshworks.fstore.es.index;
//
//import io.joshworks.fstore.es.utils.Iterators;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Optional;
//import java.util.Spliterator;
//import java.util.Spliterators;
//import java.util.UUID;
//import java.util.stream.Stream;
//import java.util.stream.StreamSupport;
//
//public class TableIndex implements Index {
//
//    private static final Logger logger = LoggerFactory.getLogger(TableIndex.class);
//
//    private static final String INDEX_FOLDER = "index";
//
//    private MemIndex memIndex = new MemIndex();
//    private final LinkedList<IndexSegment> indexSegments = new LinkedList<>();
//
//    public void add(long stream, int version, long position) {
//        if (version <= 0) {
//            throw new IllegalArgumentException("Version must be greater than zero");
//        }
//        if (position < 0) {
//            throw new IllegalArgumentException("Position must be greater than zero");
//        }
//
//        memIndex.add(stream, version, position);
//    }
//
//    @Override
//    public int version(long stream) {
//        int version = memIndex.version(stream);
//        if (version > 0) {
//            return version;
//        }
//
//        Iterator<IndexSegment> reverse = indexSegments.descendingIterator();
//        while (reverse.hasNext()) {
//            IndexSegment previous = reverse.next();
//            int v = previous.version(stream);
//            if (v > 0) {
//                return v;
//            }
//        }
//        return 0;
//    }
//
//    @Override
//    public int size() {
//        int segmentsSize = indexSegments.stream().mapToInt(IndexSegment::size).sum();
//        return segmentsSize + memIndex.size();
//    }
//
//    public long inMemoryItems() {
//        return memIndex.size();
//    }
//
//    public void flush(File directory) {
//        //TODO improve index name
//        String indexName = UUID.randomUUID().toString().substring(0, 8) + ".idx";
//        String indexLocation = INDEX_FOLDER + File.separator + indexName;
//
//        logger.info("Flushing index to {}", indexName);
//
//        File indexDir = new File(directory, indexLocation);
//        IndexSegment indexSegment = IndexSegment.write(memIndex, indexDir);
//
//        indexSegments.add(indexSegment);
//        memIndex = new MemIndex();
//    }
//
//    @Override
//    public void close() {
//        memIndex.close();
//        for (IndexSegment indexSegment : indexSegments) {
//            logger.info("Closing {}", indexSegment);
//            indexSegment.close();
//        }
//    }
//
//    @Override
//    public Stream<IndexEntry> stream() {
//        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED), false);
//    }
//
//    @Override
//    public Stream<IndexEntry> stream(Range range) {
//        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(range), Spliterator.ORDERED), false);
//    }
//
//    @Override
//    public Iterator<IndexEntry> iterator(Range range) {
//        List<Iterator<IndexEntry>> iterators = new ArrayList<>();
//
//        Iterator<IndexEntry> cacheIterator = memIndex.iterator(range);
//
//        iterators.add(cacheIterator);
//        for (IndexSegment next : indexSegments) {
//            iterators.add(next.iterator(range));
//        }
//
//        return Iterators.concat(iterators);
//    }
//
//    @Override
//    public Optional<IndexEntry> get(long stream, int version) {
//        Optional<IndexEntry> fromMemory = memIndex.get(stream, version);
//        if (fromMemory.isPresent()) {
//            return fromMemory;
//        }
//        for (IndexSegment next : indexSegments) {
//            Optional<IndexEntry> fromDisk = next.get(stream, version);
//            if (fromDisk.isPresent()) {
//                return fromDisk;
//            }
//        }
//        return Optional.empty();
//    }
//
//    @Override
//    public Iterator<IndexEntry> iterator() {
//        List<Iterator<IndexEntry>> iterators = new ArrayList<>();
//
//        for (IndexSegment next : indexSegments) {
//            iterators.add(next.iterator());
//        }
//        iterators.add(memIndex.iterator());
//
//        return Iterators.concat(iterators);
//    }
//}