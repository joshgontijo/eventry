package io.joshworks.fstore.log;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
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

    static void writeMetadata(File directory, Map<String, Object> metadata) throws IOException {
        Gson gson = new Gson();
        File file = new File(directory, METADATA_FILE);
        if(!file.exists())
            file.createNewFile();
        try (FileWriter fw = new FileWriter(file, false)) {
            gson.toJson(metadata, fw);
        }
    }

    static Map<String, Object> readMetadata(File directory) throws IOException {
        Gson gson = new Gson();
        try (FileReader fr = new FileReader(new File(directory, METADATA_FILE))) {
            return gson.fromJson(fr, new TypeToken<Map<String, Object>>() {
            }.getType());
        }
    }

    static boolean metadataExists(File directory) {
        File metadata = new File(directory, METADATA_FILE);
        return metadata.exists();
    }

}
