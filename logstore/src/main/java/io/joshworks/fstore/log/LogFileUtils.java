package io.joshworks.fstore.log;


import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.log.appender.BlockAppenderMetadata;
import io.joshworks.fstore.log.appender.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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

    public static void createRoot(File directory) {
        checkCreatePreConditions(directory);
        try {
            if (!directory.exists()) {
                Files.createDirectory(directory.toPath());
            }
            if (!new File(directory, METADATA_FILE).createNewFile()) {
                throw new RuntimeException("Failed to create metadata file");
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not create log directory", e);
        }
    }

    public static File newSegmentFile(File directory, int segmentCount) {
        String fileName = "segment_" + segmentCount + ".dat";
        File newFile = new File(directory, fileName);
        if (newFile.exists()) {
            throw new IllegalStateException("Segment file '" + fileName + "' already exist");
        }
        return newFile;
    }

    public static <T> List<Log<T>> loadSegments(File directory, Function<File, Log<T>> loader) {
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

    public static void writeMetadata(File directory, Metadata metadata) {
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(new File(directory, METADATA_FILE)))) {
            metadata.writeTo(out);
            out.flush();
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    public static void writeMetadata(File directory, BlockAppenderMetadata metadata) {
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(new File(directory, METADATA_FILE)))) {
            metadata.writeTo(out);
            out.flush();
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    public static Metadata readBaseMetadata(File directory) {
        checkOpenPreConditions(directory);
        try (DataInputStream input = new DataInputStream(new FileInputStream(new File(directory, METADATA_FILE)))) {
            return Metadata.of(input);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    public static BlockAppenderMetadata readBlockMetadata(File directory) {
        try (DataInputStream input = new DataInputStream(new FileInputStream(new File(directory, METADATA_FILE)))) {
            return BlockAppenderMetadata.of(input);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    public static void checkCreatePreConditions(File directory) {
        if (directory.exists() && !directory.isDirectory()) {
            throw new IllegalArgumentException("Destination must be a directory");
        }
//        if (directory.exists()) {
//            throw new IllegalArgumentException("Directory " + directory.getPath() + " already exist");
//        }
        if (LogFileUtils.metadataExists(directory)) {
            throw new IllegalStateException("Metadata file found, use open instead");
        }
    }

    public static void checkOpenPreConditions(File directory) {
        if (!directory.exists()) {
            throw new IllegalArgumentException("Directory doesn't exist");
        }
        if (!directory.isDirectory()) {
            throw new IllegalArgumentException(directory.getName() + " is not a directory");
        }
        if (!LogFileUtils.metadataExists(directory)) {
            throw new IllegalStateException("Metadata file not found, use create instead");
        }
    }

    public static boolean metadataExists(File directory) {
        return new File(directory, METADATA_FILE).exists();
    }

}
