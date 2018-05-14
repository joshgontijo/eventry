package io.joshworks.fstore.log;


import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;

public final class LogFileUtils {

    private static final Logger logger = LoggerFactory.getLogger(LogFileUtils.class);

    public static final String METADATA_FILE = ".metadata";
    public static final String STATE_FILE = ".state";
    private static final String SEGMENT_EXTENSION = ".lsm";

    private LogFileUtils() {

    }

    public static void createRoot(File directory) {
        checkCreatePreConditions(directory);
        try {
            if (!directory.exists()) {
                Files.createDirectories(directory.toPath());
            }
            if (!new File(directory, METADATA_FILE).createNewFile()) {
                throw new RuntimeException("Failed to create metadata file");
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not create log directory", e);
        }
    }

    public static File newSegmentFile(File directory, NamingStrategy strategy, int idxOnLevel, int level) {
        String fileName = fileName(strategy.prefix(), idxOnLevel, level);
        File newFile = new File(directory, fileName);
        if (newFile.exists()) {
            throw new IllegalStateException("Segment file '" + fileName + "' already exist");
        }
        return newFile;
    }

//    public static String fileName(String prefix, int idxOnLevel, int level) {
//        String indexOnLevel = String.format("%06d", idxOnLevel);
//        String extension = ".L" + level;
//        return  prefix + "-" + indexOnLevel + extension;
//    }

    public static String fileName(String prefix, int idxOnLevel, int level) {
        return  prefix + SEGMENT_EXTENSION;
    }



    public static File loadSegment(File directory, String name) {
        try {
//            String fileName = name.endsWith(SEGMENT_EXTENSION) ? name : name + SEGMENT_EXTENSION;
            File file = new File(directory, name);
            if(!file.exists()) {
                throw new RuntimeIOException("Segment file " + name + " doesn't exist");
            }
//            if(!isSegmentFile(name)) {
//                throw new RuntimeIOException("File " + name + " is not a valid segment file name");
//            }
            return file;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load segment " + name, e);
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

    public static boolean metadataExists(File directory) {
        return new File(directory, METADATA_FILE).exists();
    }

}
