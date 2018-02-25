package io.joshworks.fstore.log;


import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.reader.FixedBufferDataReader;
import io.joshworks.fstore.serializer.arrays.IntegerArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A Log segment that is compressed when flushed to disk. Data is kept in memory until blockSize threshold, or flush()
 */
public class BlockCompressedSegment<T> implements Log<T> {

    //[block_address(54)][entry_position(10)]
//    private static final int BLOCK_ADDRESS_BIT_SHIFT = 54;
//    private static final int BLOCK_ENTRY_POS_BIT_SHIFT = 10; //max 1024 blocks per file
    private final long maxBlockAddress;
    private final long maxEntriesPerBlock;


    private final Serializer<T> serializer;
    private final Storage storage;
    private final DataReader reader;
    private final Codec codec;
    private final int maxBlockSize;
    private final int blockBitShift;
    private final int entryIdxBitShift;
    private long nextBlockPosition; //updated on every block flush
    private Block currentBlock;
    private long entryCount;
    private long size;

    private static final Logger logger = LoggerFactory.getLogger(BlockCompressedSegment.class);

    //TODO Builder would be better here
    public static <T> BlockCompressedSegment<T> create(
            Storage storage,
            Serializer<T> serializer,
            Codec codec,
            int maxBlockSize,
            int blockBitShift,
            int entryIdxBitShift) {

        return new BlockCompressedSegment<>(storage, serializer, codec, maxBlockSize, blockBitShift, entryIdxBitShift, 0);
    }

    //so far, a block cannot be opened for writing
    public static <T> BlockCompressedSegment<T> open(Storage storage, Serializer<T> serializer, Codec codec, long position, int blockBitShift, int entryIdxBitShift) {
        return open(storage, serializer, codec, position, blockBitShift, entryIdxBitShift, false);
    }

    public static <T> BlockCompressedSegment<T> open(
            Storage storage,
            Serializer<T> serializer,
            Codec codec,
            long position,
            int blockBitShift,
            int entryIdxBitShift,
            boolean checkIntegrity) {

        BlockCompressedSegment<T> appender = null;
        try {

            appender = new BlockCompressedSegment<>(storage, serializer, codec, -1, blockBitShift, entryIdxBitShift, position);
            if (checkIntegrity) {
                appender.checkIntegrity(position);
            }
            appender.nextBlockPosition = position;
            return appender;
        } catch (CorruptedLogException e) {
            IOUtils.closeQuietly(appender);
            throw e;
        }
    }

    private BlockCompressedSegment(Storage storage, Serializer<T> serializer, Codec codec, int maxBlockSize, int blockBitShift, int entryIdxBitShift, long position) {
        this.serializer = serializer;
        this.storage = storage;
        this.reader = new FixedBufferDataReader(storage, false, 1);
        this.codec = codec;
        this.maxBlockSize = maxBlockSize;
        this.currentBlock = new Block(maxBlockSize, position);
        this.blockBitShift = blockBitShift;
        this.entryIdxBitShift = entryIdxBitShift;

        this.maxBlockAddress = (long) Math.pow(2, blockBitShift) - 1;
        this.maxEntriesPerBlock = (long) Math.pow(2, entryIdxBitShift);
    }

    private void checkIntegrity(long lastKnownPosition) {
        long position = 0;
        logger.info("Restoring log state and checking consistency until the address {}", lastKnownPosition);
        Scanner<T> scanner = scanner();
        while (scanner.hasNext()) {
            scanner.next();
            position = scanner.position();
        }
        if (position != lastKnownPosition) {
            throw new CorruptedLogException(MessageFormat.format("Expected last address {0}, got {1}", lastKnownPosition, position));
        }
        logger.info("Log state restored, current nextBlockPosition {}", position);
    }

    @Override
    public long position() {
        return nextBlockPosition;
    }

    @Override
    public T get(long position) {
        long blockAddress = getBlockAddress(position);
        Block block = Block.load(reader, codec, blockAddress);
        if (block == null) {
            return null;
        }
        int positionOnBlock = getPositionOnBlock(position);
        if (positionOnBlock < 0) {
            return null;
        }
        ByteBuffer entry = block.get(positionOnBlock);
        return serializer.fromBytes(entry);
    }

    @Override
    public T get(long position, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long entries() {
        return entryCount;
    }

    @Override
    public long size() {
        return size;
    }

    long getBlockAddress(long position) {
        long blockAddress = position >> entryIdxBitShift;
        if (blockAddress > maxBlockAddress) {
            throw new IllegalArgumentException("Invalid block address, address cannot be greater than " + maxBlockAddress);
        }
        return blockAddress;
    }

    long toBlockPosition(long position, int entryPosition) {
        if (entryPosition > maxEntriesPerBlock) {
            throw new IllegalArgumentException(MessageFormat.format("entryPosition {0} cannot be greater than {}", entryPosition, maxEntriesPerBlock));
        }
        return position << entryIdxBitShift | entryPosition;
    }

    int getPositionOnBlock(long position) {
        long mask = (1 << entryIdxBitShift) - 1;
        return (int) (position & mask);
    }

    @Override
    public long append(T data) {
        if (currentBlock.size >= maxBlockSize || currentBlock.data.size() >= maxEntriesPerBlock) {
            flush();
            currentBlock = new Block(maxBlockSize, nextBlockPosition);//with updated nextBlockPosition
        }
        ByteBuffer dataBytes = serializer.toBytes(data);
        dataBytes.flip();

        long pos = toBlockPosition(nextBlockPosition, currentBlock.data.size());
        currentBlock.add(dataBytes);
        entryCount++;
        size += dataBytes.limit();
        return pos;
    }

    @Override
    public Scanner<T> scanner() {
        return new LogReader(reader, serializer, codec);
    }

    @Override
    public Scanner<T> scanner(long position) {
        return new LogReader(reader, serializer, codec, position);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(storage);
    }

    @Override
    public void flush() {
        if(currentBlock.data.isEmpty()) {
            return;
        }
        nextBlockPosition = currentBlock.write(storage, codec, nextBlockPosition);
    }


    //NOT THREAD SAFE
    private class LogReader implements Scanner<T> {

        private final DataReader reader;
        private final Serializer<T> serializer;
        private final Codec codec;
        private T data;
        private boolean completed = false;
        private BlockCompressedSegment.Block currentBlock;
        private int blockEntryIdx;

        private LogReader(DataReader reader, Serializer<T> serializer, Codec codec) {
            this(reader, serializer, codec, 0);
        }

        private LogReader(DataReader reader, Serializer<T> serializer, Codec codec, long initialPosition) {
            this.reader = reader;
            this.serializer = serializer;
            this.codec = codec;
            this.currentBlock = Block.load(reader, codec, getBlockAddress(initialPosition));
            this.blockEntryIdx = getPositionOnBlock(initialPosition);
            if(this.currentBlock == null) {
                this.completed = true;
            }
        }

        private T readAndVerify() {
            if (blockEntryIdx >= currentBlock.entries()) {
                blockEntryIdx = 0;
                currentBlock = Block.load(reader, codec, currentBlock.nextBlock());
            }
            if(currentBlock == null) {
                return null;
            }
            if(currentBlock.entries() == 0) {
                return null; //last empty block
            }

            ByteBuffer data = currentBlock.get(blockEntryIdx++);
            data.clear();

            ByteBuffer dataBuffer = ByteBuffer.allocate(data.remaining());
            dataBuffer.put(data);
            dataBuffer.flip();

            return serializer.fromBytes(dataBuffer);
        }

        @Override
        public boolean hasNext() {
            if (completed) {
                return false;
            }
            this.data = readAndVerify();
            this.completed = this.data == null;
            return !completed;

        }

        @Override
        public T next() {
            if(data == null) {
                throw new NoSuchElementException();
            }
            return data;
        }

        @Override
        public long position() {
            return nextBlockPosition;
        }

        @Override
        public Iterator<T> iterator() {
            return this;
        }
    }


    private static class Block {

        private static final int READ_ONLY = -1;

        private final List<ByteBuffer> data;
        private int compressedSize = -1;
        private int size;
        private final int maxSize;
        private final long address;

        private static final Serializer<int[]> lengthSerializer = new IntegerArraySerializer();

        private Block(int maxSize, long address) {
            this(maxSize, address, new ArrayList<>());
        }

        private Block(int maxSize, long address, List<ByteBuffer> data) {
            this.maxSize = maxSize;
            this.address = address;
            this.data = data;
        }

        private static List<ByteBuffer> decompressing(Codec decompressor, ByteBuffer buffer) {
            ByteBuffer decompressed = decompressor.decompress(buffer);
            int[] lengths = lengthSerializer.fromBytes(decompressed);
            List<ByteBuffer> buffers = new LinkedList<>();

            for (int length : lengths) {
                byte[] dataArray = new byte[length];
                decompressed.get(dataArray);
                buffers.add(ByteBuffer.wrap(dataArray));
            }

            return Collections.unmodifiableList(buffers);
        }

        private static Block load(DataReader reader, Codec codec, long blockAddress) {

            ByteBuffer read = reader.read(blockAddress);
            if(read.limit() == 0) {
                return null;
            }

            int size = read.remaining();
            List<ByteBuffer> entries = decompressing(codec, read);

            Block block = new Block(READ_ONLY, blockAddress, entries);
            block.compressedSize = size;
            return block;
        }

        void add(ByteBuffer buffer) {
            if (maxSize == READ_ONLY) {
                throw new IllegalStateException("Block is read only");
            }
            buffer.clear();
            size += buffer.limit();
            data.add(buffer);
        }

        long write(Storage storage, Codec compressor, long position) {
            if (maxSize == READ_ONLY) {
                throw new IllegalStateException("Block is not writable");
            }
            int[] lengths = data.stream().mapToInt(bb -> bb.clear().remaining()).toArray();
            ByteBuffer lengthData = lengthSerializer.toBytes(lengths);

            ByteBuffer withLength = ByteBuffer.allocate(size + lengthData.remaining());
            withLength.put(lengthData); //length + array of int
            for (ByteBuffer entry : data) {
                withLength.put(entry);
            }

            withLength.flip();
            ByteBuffer compressed = compressor.compress(withLength);

            return Log.write(storage, compressed, position);
        }

        private ByteBuffer get(int index) {
            return data.get(index);
        }

        public int size() {
            return size;
        }

        public int entries() {
            return data.size();
        }

        public long nextBlock() {
            return address + HEADER_SIZE + compressedSize;
        }

    }

}
