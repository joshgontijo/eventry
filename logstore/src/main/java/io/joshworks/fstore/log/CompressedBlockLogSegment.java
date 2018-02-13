package io.joshworks.fstore.log;


import io.joshworks.fstore.api.Codec;
import io.joshworks.fstore.api.Serializer;
import io.joshworks.fstore.serializer.arrays.IntegerArraySerializer;
import io.joshworks.fstore.utils.IOUtils;
import io.joshworks.fstore.utils.io.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A Log segment that is compressed when flushed to disk. Data is kept in memory until blockSize threshold, or flush()
 */
public class CompressedBlockLogSegment<T> implements Log<T> {

    //[block_address(54)][entry_position(10)]
//    private static final int BLOCK_ADDRESS_BIT_SHIFT = 54;
//    private static final int BLOCK_ENTRY_POS_BIT_SHIFT = 10; //max 1024 blocks per file
    private final long maxBlockAddress;
    private final long maxEntriesPerBlock;

    //TODO move to parent ?
    private static final int HEADER_SIZE = Integer.BYTES + Integer.BYTES; //length + checksum

    private final Serializer<T> serializer;
    private final Storage storage;
    private final Codec codec;
    private final int maxBlockSize;
    private final int blockBitShift;
    private final int entryIdxBitShift;
    private long position;
    private Block currentBlock;

    private static final Logger logger = LoggerFactory.getLogger(CompressedBlockLogSegment.class);


    //TODO Builder would be better here
    public static <T> CompressedBlockLogSegment<T> create(
            Storage storage,
            Serializer<T> serializer,
            Codec codec,
            int maxBlockSize,
            int blockBitShift,
            int entryIdxBitShift) {

        return new CompressedBlockLogSegment<>(storage, serializer, codec, maxBlockSize, blockBitShift, entryIdxBitShift);
    }

    //so far, a block cannot be opened for writing
    public static <T> CompressedBlockLogSegment<T> open(Storage storage, Serializer<T> serializer, Codec codec, long position, int blockBitShift, int entryIdxBitShift) {
        return open(storage, serializer, codec, position, blockBitShift, entryIdxBitShift, false);
    }

//    public static void main(String[] args) {
//
//        System.out.println(maxBlockAddress);
//        System.out.println(MAX_ENTRY_PER_BLOCK);
//
//        long a = 12345;
//        long valueA = 9;
//        int shift = 4;
//        long b = a << shift | valueA;
//
//        long mask = (1 << shift) - 1;
//        long c = b & mask;
//        long d = b >> shift;
//        System.out.println(Long.toBinaryString(a));
//        System.out.println(Long.toBinaryString(valueA));
//        System.out.println(Long.toBinaryString(b));
//        System.out.println(Long.toBinaryString(mask));
//        System.out.println(Long.toBinaryString(c));
//        System.out.println(Long.toBinaryString(d));
//    }

    public static <T> CompressedBlockLogSegment<T> open(
            Storage storage,
            Serializer<T> serializer,
            Codec codec,
            long position,
            int blockBitShift,
            int entryIdxBitShift,
            boolean checkIntegrity) {

        CompressedBlockLogSegment<T> appender = null;
        try {

            appender = new CompressedBlockLogSegment<>(storage, serializer, codec, -1, blockBitShift, entryIdxBitShift);
            if (checkIntegrity) {
                appender.checkIntegrity(position);
            }
            appender.position = position;
            return appender;
        } catch (CorruptedLogException e) {
            IOUtils.closeQuietly(appender);
            throw e;
        }
    }

    private CompressedBlockLogSegment(Storage storage, Serializer<T> serializer, Codec codec, int maxBlockSize, int blockBitShift, int entryIdxBitShift) {
        this.serializer = serializer;
        this.storage = storage;
        this.codec = codec;
        this.maxBlockSize = maxBlockSize;
        this.currentBlock = new Block(maxBlockSize);
        this.blockBitShift = blockBitShift;
        this.entryIdxBitShift = entryIdxBitShift;
        this.maxBlockAddress = (long) Math.pow(2, blockBitShift) - 1;
        this.maxEntriesPerBlock = (long) Math.pow(2, entryIdxBitShift);
    }


    private void checkIntegrity(long lastKnownPosition) {
        long position = 0;
        logger.info("Restoring log state and checking consistency until the position {}", lastKnownPosition);
        Scanner<T> scanner = scanner();
        while (scanner.hasNext()) {
            scanner.next();
            position = scanner.position();
        }
        if (position != lastKnownPosition) {
            throw new CorruptedLogException(MessageFormat.format("Expected last position {0}, got {1}", lastKnownPosition, position));
        }
        logger.info("Log state restored, current position {}", position);
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public T get(long position) {
        long blockAddress = getBlockAddress(position);
        Block block = loadBlock(blockAddress);
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

    long getBlockAddress(long position) {
        long blockAddress = position >> entryIdxBitShift;
        if (blockAddress > maxBlockAddress) {
            throw new IllegalArgumentException("Invalid block address, address cannot be greater than " + maxBlockAddress);
        }
        return blockAddress;
    }

    long toAbsolutePosition(long position, int entryPosition) {
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
        ByteBuffer dataBytes = serializer.toBytes(data);

        dataBytes.flip();
        if (currentBlock.size >= maxBlockSize || currentBlock.data.size() >= maxEntriesPerBlock) {
            flush();
            currentBlock = new Block(maxBlockSize);
        }
        long pos = toAbsolutePosition(position, currentBlock.data.size());
        currentBlock.add(dataBytes);
        return pos;
    }

    @Override
    public Scanner<T> scanner() {
        return new LogReader<>(storage, serializer);
    }

    @Override
    public Scanner<T> scanner(long position) {
        return new LogReader<>(storage, serializer, position);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(storage);
    }

    @Override
    public void flush() {
        ByteBuffer compressed = currentBlock.compress(codec);

        position += storage.write(position, compressed);
    }

    private Block loadBlock(long blockAddress) {


        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        int read = storage.read(blockAddress, header);
        if (read <= 0) {
            return null;
        }
        header.flip();
        int blockSize = header.getInt();
        int checksum = header.getInt();

        ByteBuffer blockData = ByteBuffer.allocate(blockSize);
        storage.read(blockAddress + HEADER_SIZE, blockData);
        blockData.flip();

        if (Checksum.checksum(blockData) != checksum) {
            throw new ChecksumException();
        }
        return Block.decompressing(codec, blockData);
    }

    //NOT THREAD SAFE
    private static class LogReader<T> implements Scanner<T> {

        private final Storage storage;
        private final Serializer<T> serializer;
        private T data;
        private boolean completed = false;
        private long position;

        private LogReader(Storage storage, Serializer<T> serializer) {
            this(storage, serializer, 0);
        }

        private LogReader(Storage storage, Serializer<T> serializer, long initialPosition) {
            this.storage = storage;
            this.serializer = serializer;
            this.position = initialPosition;
        }

        private T readAndVerify() {
            long currentPos = position;
            if (currentPos + HEADER_SIZE > storage.size()) {
                return null;
            }

            //TODO read may return less than the actual dataBuffer size / or zero ?
            ByteBuffer heading = ByteBuffer.allocate(HEADER_SIZE);
            currentPos += storage.read(currentPos, heading);

            heading.flip();
            int length = heading.getInt();
            if (length <= 0) {
                return null;
            }
            if (currentPos + length > storage.size()) {
                throw new IllegalStateException("Not data to be read");
            }

            int writeChecksum = heading.getInt();

            ByteBuffer dataBuffer = ByteBuffer.allocate(length);
            currentPos += storage.read(currentPos, dataBuffer);
            position = currentPos;

            dataBuffer.flip();

            int readChecksum = Checksum.checksum(dataBuffer);
            if (readChecksum != writeChecksum) {
                throw new ChecksumException();
            }

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
            return this.data;
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public Iterator<T> iterator() {
            return this;
        }
    }


    private static class Block {

        private static final int READ_ONLY = -1;

        private final List<ByteBuffer> data;
        private int size;
        private final int maxSize;

        private static final Serializer<int[]> lengthSerializer = new IntegerArraySerializer();

        private Block(int maxSize) {
            this(maxSize, new LinkedList<>());
        }

        private Block(int maxSize, List<ByteBuffer> data) {
            this.maxSize = maxSize;
            this.data = data;
            this.size = data.stream().mapToInt(ByteBuffer::limit).sum();
        }

        public static Block readOnly(List<ByteBuffer> data) {
            return new Block(READ_ONLY, data);
        }

        public static Block decompressing(Codec decompressor, ByteBuffer buffer) {
            ByteBuffer decompressed = ByteBuffer.wrap(decompressor.decompress(buffer.array()));
            int[] lengths = lengthSerializer.fromBytes(decompressed);
            List<ByteBuffer> buffers = new LinkedList<>();

            for (int i = 0; i < lengths.length; i++) {
                byte[] dataArray = new byte[lengths[i]];
                decompressed.get(dataArray);
                buffers.add(ByteBuffer.wrap(dataArray));
            }

            return new Block(-1, Collections.unmodifiableList(buffers));
        }

        public void add(ByteBuffer buffer) {
            if (maxSize == READ_ONLY) {
                throw new IllegalStateException("Block is read only");
            }
            buffer.clear();
            data.add(buffer);
            size += buffer.remaining();
        }

        public ByteBuffer compress(Codec compressor) {
            if (maxSize == READ_ONLY) {
                throw new IllegalStateException("Block is not writable");
            }
            int[] lengths = data.stream().mapToInt(bb -> bb.clear().remaining()).toArray();
            ByteBuffer lengthData = lengthSerializer.toBytes(lengths);

            ByteBuffer withLength = ByteBuffer.allocate(size + lengthData.remaining());
            withLength.put(lengthData); //length + array of int
            for (ByteBuffer datum : data) {
                withLength.put(datum);
            }

            withLength.flip();
            byte[] compressed = compressor.compress(withLength.array());
            ByteBuffer finalData = ByteBuffer.allocate(HEADER_SIZE + compressed.length);
            finalData.putInt(compressed.length);
            finalData.putInt(Checksum.checksum(compressed));
            finalData.put(compressed);
            finalData.flip();
            return finalData;
        }

        private ByteBuffer get(int index) {
            return data.get(index);
        }

        public int size() {
            return size;
        }

    }

}
