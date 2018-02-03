package io.joshworks.logstore;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileLogStore implements LogStore {

    private final FileChannel channel;

    public FileLogStore(String name, String mode) {
        try {
            channel = new RandomAccessFile(name, mode).getChannel();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public synchronized long append(ByteBuffer data) throws IOException {
        long position = channel.position();
        channel.write(data);
        return position;
    }


    @Override
    public synchronized int read(ByteBuffer data, int offset) throws IOException {
        return channel.read(data, offset);
    }

    @Override
    public void close() {
        try {
            channel.close();
            //TODO deleting the file since it's just testing atm
//            Files.delete(file.toPath());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
