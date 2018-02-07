package io.joshworks.fstore.log;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class LogFileUtils {
    private LogFileUtils() {

    }

    private static final Logger logger = LoggerFactory.getLogger(LogFileUtils.class);

    private static final String METADATA_FILE = "metadata.dat";
    private static final String LATEST_SEGMENT = "metadata.dat";

    static void createRoot(File directory) {
        try {
            if (!directory.exists()) {
                if(!directory.mkdir()) {
                    throw new IllegalStateException("Could not create root directory");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not create log directory", e);
        }
    }

    static File newSegmentFile(File directory, int segmentCount) {
        String fileName = "segment_" + segmentCount + ".dat";
        File newFile = new File(directory, fileName);
        if (newFile.exists()) {
            throw new IllegalStateException("Segment file '" + fileName + "' already exist");
        }
        return newFile;
    }

    static <T> List<Log<T>> loadSegments(File directory, Function<File, Log<T>> loader) {
        try {
            return Files.list(directory.toPath())
                    .filter(p -> p.getFileName().startsWith("segment_") && p.getFileName().endsWith(".dat"))
                    .map(Path::toFile)
                    .map(loader)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to load segments", e);
        }
    }

    static void writeMetadata(File directory, RollingLogAppender.Metadata metadata) throws IOException {
        try (OutputStream os = new FileOutputStream(new File(directory, METADATA_FILE))) {
            DataOutput baos = new DataOutputStream(os);
            baos.writeLong(metadata.lastPosition);
            baos.writeInt(metadata.segmentSize);
        }
    }

    static RollingLogAppender.Metadata readMetadata(File directory) throws IOException {
        try (InputStream os = new FileInputStream(new File(directory, METADATA_FILE))) {
            DataInput input = new DataInputStream(os);
            return new RollingLogAppender.Metadata(input.readLong(), input.readInt());
        }
    }

    static boolean metadataExists(File directory) {
        return new File(directory, METADATA_FILE).exists();
    }

}
