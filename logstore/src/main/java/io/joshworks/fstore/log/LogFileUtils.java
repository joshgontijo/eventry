package io.joshworks.fstore.log;


import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.log.appender.BlockAppenderMetadata;
import io.joshworks.fstore.log.appender.Metadata;
import io.joshworks.fstore.log.appender.State;
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
import java.util.stream.Collectors;

public final class LogFileUtils {

    private static final Logger logger = LoggerFactory.getLogger(LogFileUtils.class);

    private static final String METADATA_FILE = "metadata.dat";
    private static final String STATE_FILE = "state.dat";
    private static final String SEGMENT_EXTENSION = ".dat";
    private static final String SEGMENT_PREFIX = "segment_";

    private LogFileUtils() {

    }

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

    public static File newSegmentFile(File directory, long maxSegments, int segmentCount) {
        String fileName = segmentName(maxSegments, segmentCount);
        File newFile = new File(directory, fileName);
        if (newFile.exists()) {
            throw new IllegalStateException("Segment file '" + fileName + "' already exist");
        }
        return newFile;
    }

    private static String segmentName(long maxSegmentSize, int segmentIdx) {
        long totalLength = (long) (Math.log10(maxSegmentSize) + 1);
        String format = SEGMENT_PREFIX + "%0" + (totalLength) + "d" + SEGMENT_EXTENSION;
        return String.format(format, segmentIdx);
    }

    public static List<File> loadSegments(File directory) {
        try {
            return Files.list(directory.toPath())
                    .filter(p -> isSegmentFile(p.getFileName().toFile().getName()))
                    .map(Path::toFile)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to load segments", e);
        }
    }

    public static void tryCreateMetadata(File directory, Metadata metadata) {
        try {
            LogFileUtils.writeMetadata(directory, metadata);
        } catch (RuntimeIOException e) {
            try {
                Files.delete(directory.toPath());
            } catch (IOException e1) {
                logger.error("Failed to revert directory creation: " + directory.getPath());
            }
            throw e;
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
            return Metadata.readFrom(input);
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

    public static void writeState(File directory, State state) {
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(new File(directory, STATE_FILE)))) {
            state.writeTo(out);
            out.flush();
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    public static State readState(File directory) {
        try (DataInputStream input = new DataInputStream(new FileInputStream(new File(directory, STATE_FILE)))) {
            return State.readFrom(input);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    public static void checkCreatePreConditions(File directory) {
        if (directory.exists() && !directory.isDirectory()) {
            throw new IllegalArgumentException("Destination must be a directory");
        }

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

    public static void purge(File directory) throws IOException {
        String[] files = directory.list();
        if (files != null) {
            for (String s : files) {
                logger.info("Deleting '{}'", s);
                Files.delete(new File(directory, s).toPath());
            }
        }
        Files.delete(directory.toPath());
    }

    public static boolean metadataExists(File directory) {
        return new File(directory, METADATA_FILE).exists();
    }

    private static boolean isSegmentFile(String name) {
        return name.startsWith(SEGMENT_PREFIX) && name.endsWith(SEGMENT_EXTENSION);
    }
}
