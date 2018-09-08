package io.joshworks.fstore.es.tools;

import io.joshworks.fstore.es.EventStore;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class LogDump {

    public static void dumpStream(String stream, File file, EventStore store) {
        try (var fileWriter = new FileWriter(file)) {
            LogIterator<EventRecord> iterator = store.fromStreamIter(stream);
            while (iterator.hasNext()) {
                EventRecord event = iterator.next();
                fileWriter.write(event.toString() + System.lineSeparator());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void dumpLog(File file, EventStore store) {
        try (var fileWriter = new FileWriter(file)) {
            LogIterator<EventRecord> iterator = store.fromAllIter();
            while (iterator.hasNext()) {
                long position = iterator.position();
                EventRecord event = iterator.next();
                fileWriter.write(position + " | " + event.toString() + System.lineSeparator());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void dumpIndex(File file, EventStore store) {
        try (var fileWriter = new FileWriter(file)) {
            LogIterator<IndexEntry> iterator = store.keys();
            while (iterator.hasNext()) {
                long position = iterator.position();
                IndexEntry event = iterator.next();
                fileWriter.write(position + " | " +event.toString() + System.lineSeparator());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
