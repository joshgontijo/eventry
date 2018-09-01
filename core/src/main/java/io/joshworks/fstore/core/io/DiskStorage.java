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

    protected Mode mode;
    protected RandomAccessFile raf;
    protected FileChannel channel;
    protected File file;
    protected FileLock lock;
    protected long position;

    public DiskStorage(File target, long length, Mode mode) {
        this.mode = mode;
        Objects.requireNonNull(target, "File must specified");
        logger.info("Opening {}, length: {}", target.getName(), length);
        if (length <= 0) {
            throw new StorageException("File length must be specified");
        }

        if (length < target.length()) {
            logger.error("The specified ({}) is less than the actual file length ({}), this may cause loss of data, use 'truncate()' instead", length, target.length());
            throw new StorageException("The specified length (" + length + ") is less than the actual file length (" + target.length() + ")");
        }

        this.raf = IOUtils.randomAccessFile(target, mode);
        try {
            if (length != target.length() && Mode.READ_WRITE.equals(mode)) {
                this.raf.setLength(length);
            }
            this.file = target;
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
        if(Mode.READ.equals(mode)) {
            throw new IllegalStateException("Cannot update position on read mode");
        }
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
            close();
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
    public void truncate(long pos) {
        if(Mode.READ.equals(mode)) {
            throw new StorageException("Cannot truncate readonly file");
        }
        try {
            channel.truncate(pos);
            this.position = position > pos ? pos : position;
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void markAsReadOnly() {
        mode = Mode.READ;
    }

}
