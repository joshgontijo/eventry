package io.joshworks.fstore.log;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public static File newSegmentFile(File directory, NamingStrategy strategy, int level) {
        String fileName = fileName(strategy.prefix(), level);
        File newFile = new File(directory, fileName);
        if (newFile.exists()) {
            throw new IllegalStateException("Segment file '" + fileName + "' already exist");
        }
        return newFile;
    }

    public static String fileName(String prefix, int level) {
        String extension = ".L" + level;
        return  prefix +  extension;
    }

//    public static String fileName(String prefix, int idxOnLevel, int level) {
//        return  prefix + SEGMENT_EXTENSION;
//    }

    public static File getSegmentHandler(File directory, String name) {
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

//    public static Map<Integer, List<String>> findSegments(File directory) {
//
//        final String extPattern = ".L?[0-9.]+$";
//
//        try (Stream<Path> files = Files.list(directory.toPath())) {
//            List<Path> segments = files.filter(path -> extensionOf(path).matches(extPattern)).collect(Collectors.toList());
//
//
//            Map<Integer, List<String>> levels = new HashMap<>();
//            for (Path segment : segments) {
//                String ext = extensionOf(segment);
//                int level = Integer.parseInt(ext.substring(1, ext.length()));
//                levels.putIfAbsent(level, new ArrayList<>());
//                levels.get(level).add(segment.getFileName().toString());
//            }
//
//            return levels;
//
//
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }

    public static List<String> findSegments(File directory) {
        final String extPattern = ".L?[0-9.]+$";
        try (Stream<Path> files = Files.list(directory.toPath())) {
            return files.filter(path -> extensionOf(path).matches(extPattern)).map(p -> p.getFileName().toString()).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String extensionOf(Path path) {
        String filename = path.getFileName().toString();
        String[] split = filename.split("\\.");
        return split.length != 2 ? "" : split[1];
    }

    public static void checkCreatePreConditions(File directory) {
        if (directory.exists() && !directory.isDirectory()) {
            throw new IllegalArgumentException("Destination must be a directory");
        }

        if (LogFileUtils.metadataExists(directory)) {
            throw new IllegalStateException("Metadata file found, use create instead");
        }
    }

    public static boolean metadataExists(File directory) {
        return new File(directory, METADATA_FILE).exists();
    }

}
