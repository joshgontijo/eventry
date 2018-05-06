package io.joshworks.fstore.core.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.util.Objects;

public abstract class DiskStorage implements Storage {

    private static final Logger logger = LoggerFactory.getLogger(DiskStorage.class);

    protected RandomAccessFile raf;
    protected FileChannel channel;
    protected File file;
    protected FileLock lock;
    protected long position;

    public DiskStorage(File target, long length) {
        Objects.requireNonNull(target, "File must specified");
        logger.info("Opening {}", target.getName());
        if (length <= 0) {
            throw new StorageException("File length must be specified");
        }

        if (length < target.length()) {
            logger.error("The specified ({}) is less than the actual file length ({}), this may cause loss of data, use 'shrink()' instead", length, target.length());
            throw new StorageException("The specified length (" + length + ") is less than the actual file length (" + target.length() + ")");
        }

        this.raf = IOUtils.readWriteRandomAccessFile(target);
        try {
            this.raf.setLength(length);
            this.file = target;
            this.raf.setLength(length);
            this.channel = raf.getChannel();
            this.lock = this.channel.lock();

        } catch (Exception e) {
            IOUtils.closeQuietly(raf);
            IOUtils.closeQuietly(channel);
            IOUtils.releaseLock(lock);
            throw new StorageException("Failed to open storage of " + target.getName(), e);
        }
    }

    protected void ensureNonEmpty(ByteBuffer data) {
        if (data.remaining() == 0) {
            throw new IllegalArgumentException("Cannot store empty record");
        }
    }

    @Override
    public long length() {
        try {
            return raf.length();
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    public void position(long position) {
        try {
            channel.position(position);
            this.position = position;
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public void delete() {
        try {
            IOUtils.closeQuietly(channel);
            IOUtils.closeQuietly(raf);
            Files.delete(file.toPath());
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void close() {
        flush();
        if (channel.isOpen()) {
            IOUtils.releaseLock(lock);
        }
        IOUtils.closeQuietly(channel);
        IOUtils.closeQuietly(raf);
    }

    @Override
    public void flush() {
        try {
            if (channel.isOpen()) {
                channel.force(true);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public String name() {
        return file.getName();
    }

    @Override
    public void shrink() {
        try {
            raf.setLength(position);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    public static class StorageException extends RuntimeException {

        public StorageException(String message) {
            super(message);
        }

        public StorageException(String message, Throwable cause) {
            super(message, cause);
        }

        public StorageException(Throwable cause) {
            super(cause);
        }

    }

}
