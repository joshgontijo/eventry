package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.reader.FixedBufferDataReader;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.serializer.VStringSerializer;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

//The segment history
public class History {

    private static final String FILE_NAME = ".history";
    private final Segment<SegmentStateChange> history;

    History(File directory, String magic) {
        Storage storage = new RafStorage(new File(directory, FILE_NAME), 4096, Mode.READ_WRITE);
        this.history = new Segment<>(storage, new HistorySerializer(), new FixedBufferDataReader(false), magic, Type.LOG_HEAD);
    }

    public synchronized void push(SegmentStateChange change) {
        history.append(change);
    }

    public List<List<String>> rebuildState() {

        LogIterator<SegmentStateChange> iterator = history.iterator();

        Map<Integer, List<String>> levels = new HashMap<>();

        while (iterator.hasNext()) {
            SegmentStateChange change = iterator.next();
            apply(change, levels);
        }

//        Collections.sort();

        return null;

    }

    private void apply(SegmentStateChange change, Map<Integer, List<String>> levels) {
        switch (change.id) {
            case SegmentAdded.TYPE:
                levels.putIfAbsent(change.level, new ArrayList<>());
                levels.get(change.level).add(change.data);
                break;
            case SegmentDeleted.TYPE:
                List<String> values = levels.get(change.level);
                values.remove(change.data);
                break;
            case SegmentMerged.TYPE:
                List<String> sources = SegmentMerged.getSources(change.data);
                String result = SegmentMerged.getResultSegment(change.data);
                for (String source : sources) {
                    apply(new SegmentDeleted(source, change.ref, change.level), levels);
                }
                apply(new SegmentAdded(result, change.ref, change.level + 1), levels);
                break;
            default:
                throw new IllegalStateException("Invalid event type " + change.id);
        }
    }

    public static class SegmentStateChange {

        protected String id;
        protected String data;
        protected String ref;
        protected int level;

        protected SegmentStateChange(String id, String data, String ref, int level) {
            this.id = id;
            this.data = data;
            this.ref = ref;
            this.level = level;
        }
    }

    public static class SegmentDeleted extends SegmentStateChange {

        private static final String TYPE = "SEGDEL";

        public SegmentDeleted(String name, String ref, int level) {
            super(TYPE, name, ref, level);
        }
    }

    public static class SegmentAdded extends SegmentStateChange {

        private static final String TYPE = "SEGADD";

        protected SegmentAdded(String name, String ref, int level) {
            super(TYPE, name, ref, level);
        }
    }

    public static class SegmentMerged extends SegmentStateChange {

        private static final String TYPE = "SEGMRG";
        private static final String SRC_DST_SEPARATOR = "|";
        private static final String DST_SEPARATOR = ",";

        private SegmentMerged(String name, String ref, int level) {
            super(TYPE, name, ref, level);
        }

        public static SegmentMerged create(String name, String ref, int level, List<String> sources) {
            StringJoiner joiner = new StringJoiner(DST_SEPARATOR);
            for (String source : sources) {
                joiner.add(source);
            }

            String data = name + SRC_DST_SEPARATOR + joiner.toString();
            return new SegmentMerged(data, ref, level);
        }

        private static List<String> getSources(String value) {
            String[] split = value.split(SRC_DST_SEPARATOR)[1].split(DST_SEPARATOR);
            return Arrays.asList(split);
        }

        private static String getResultSegment(String value) {
            return value.split(SRC_DST_SEPARATOR)[0];
        }


    }

    private static class HistorySerializer implements Serializer<SegmentStateChange> {

        private static final Serializer<String> stringSerializer = new VStringSerializer();

        @Override
        public ByteBuffer toBytes(SegmentStateChange data) {

            ByteBuffer bb = ByteBuffer.allocate(VStringSerializer.sizeOf(data.id) + VStringSerializer.sizeOf(data.data) + VStringSerializer.sizeOf(data.ref) + Integer.BYTES);

            writeTo(data, bb);

            bb.flip();
            return bb;

        }

        @Override
        public void writeTo(SegmentStateChange data, ByteBuffer dest) {
            stringSerializer.writeTo(data.id, dest);
            stringSerializer.writeTo(data.data, dest);
            stringSerializer.writeTo(data.ref, dest);
            stringSerializer.writeTo(data.ref, dest);
        }

        @Override
        public SegmentStateChange fromBytes(ByteBuffer buffer) {
            String id = stringSerializer.fromBytes(buffer);
            String data = stringSerializer.fromBytes(buffer);
            String ref = stringSerializer.fromBytes(buffer);
            int level = buffer.getInt();


            return new SegmentStateChange(id, data, ref, level);
        }
    }

}
