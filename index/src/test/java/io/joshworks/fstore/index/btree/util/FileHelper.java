package io.joshworks.fstore.index.btree.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class FileHelper {


    private static final String FILE_DIR = "testFiles";
    private static final String EXT = ".txt";

    public static void saveTestData(int order, LinkedList<Integer> data, Integer failedValue) throws IOException {
        System.err.println("FAILED  -  ORDER: " + order + "  NUM_KEYS: " + data.size() + "  VALUE: " + failedValue);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(FILE_DIR + File.separator + toFileName(System.currentTimeMillis())))) {

            bw.write(order + "");
            bw.newLine();

            bw.write(failedValue == null ? "" : failedValue + "");
            bw.newLine();
            bw.newLine();

            for (Integer val : data) {
                bw.write(val + "");
                bw.newLine();
            }

        }
    }

    public static List<String> all() {
        return filesAsTimestamps().mapToObj(FileHelper::toFileName).collect(Collectors.toList());
    }

    public static TestData loadTestData(String name) {

        try (BufferedReader br = new BufferedReader(new FileReader(FILE_DIR + File.separator + name))) {
            TestData testData = new TestData();
            testData.order = Integer.parseInt(br.readLine());

            String failedValue = br.readLine();
            if (!failedValue.isEmpty()) {
                testData.failedValue = Integer.parseInt(failedValue);
            }

            String ignore = br.readLine();
            testData.data = new LinkedList<>();
            String line;
            while ((line = br.readLine()) != null) {
                testData.data.add(Integer.parseInt(line));
            }

            return testData;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String latest() {
        OptionalLong max = filesAsTimestamps().max();
        if(!max.isPresent()) {
            return null; //no file was found, test should ignore
        }
        return String.valueOf(filesAsTimestamps().max()) + EXT;
    }

    private static LongStream filesAsTimestamps() {
        File file = new File(FILE_DIR);
        String[] files = file.list();
        if(files == null) {
            return LongStream.empty();
        }
        return Stream.of(files).mapToLong(f -> Long.valueOf(f.split("\\.")[0]));
    }

    private static String toFileName(long timestamp) {
        return timestamp + EXT;
    }






}
