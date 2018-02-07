package io.joshworks.fstore.log;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import static java.nio.file.FileVisitResult.CONTINUE;

public class Utils {

    //terrible work around for waiting the mapped buffer to release file lock
    static void tryRemoveFile(File file) {
        int maxTries = 5;
        int counter = 0;
        while (counter++ < maxTries) {
            try {
                if (file.isDirectory()) {
                    String[] list = file.list();
                    if(list != null)
                        for (String f : list) {
                            Files.delete(new File(file, f).toPath());
                        }
                }
                Files.delete(file.toPath());
                break;
            } catch (Exception e) {
                System.err.println(":: LOCK NOT RELEASED YET ::");
                e.printStackTrace();
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    static void removeFiles(File directory) throws IOException {
        String[] files = directory.list();
        if(files == null) {
            return;
        }
        for (String s : files) {
            Files.delete(new File(directory, s).toPath());
        }
    }

    private static void deleteDirectory(File dir) throws IOException {
        Files.walkFileTree(dir.toPath(), new SimpleFileVisitor<>() {

            @Override
            public FileVisitResult visitFile(Path file,
                                             BasicFileAttributes attrs) throws IOException {

                System.out.println("Deleting file: " + file);
                Files.delete(file);
                return CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir,
                                                      IOException exc) throws IOException {

                System.out.println("Deleting dir: " + dir);
                if (exc == null) {
                    Files.delete(dir);
                    return CONTINUE;
                } else {
                    throw exc;
                }
            }

        });
    }
}
