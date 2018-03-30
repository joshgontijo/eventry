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

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A Log segment that is compressed when flushed to disk. Data is kept in memory until blockSize threshold, or flush()
 *
 * Address format
 *
 * [BLOCK_ADDRESS] [ENTRY_IDX_ON_BLOCK]
 *
 */
public class BlockCompressedSegment<T> implements Log<T> {


    private final Serializer<T> serializer;
    private final Storage storage;
    private final DataReader reader;
    private final Codec codec;

    private final int entryIdxBitShift;

    final int maxBlockSize;
    final long maxBlockAddress;
    final long maxEntriesPerBlock;

    private long blockStartPosition; //updated on every block flush
    private Block currentBlock;
    private long size;

    private static final Logger logger = LoggerFactory.getLogger(BlockCompressedSegment.class);

    //TODO Builder would be better here
    public static <T> BlockCompressedSegment<T> create(
            Storage storage,
            Serializer<T> serializer,
            Codec codec,
            int maxBlockSize,
            long maxBlockAddress,
            int entryIdxBitShift) {

        return new BlockCompressedSegment<>(storage, serializer, codec, maxBlockSize, maxBlockAddress, entryIdxBitShift, 0);
    }

    public static <T> BlockCompressedSegment<T> open(
            Storage storage,
            Serializer<T> serializer,
            Codec codec,
            int maxBlockSize,
            long maxBlockAddress,
            int entryIdxBitShift,
            long position) {

        try {
            BlockCompressedSegment<T> appender = new BlockCompressedSegment<>(storage, serializer, codec, maxBlockSize, maxBlockAddress, entryIdxBitShift, position);
            appender.blockStartPosition = position;
            return appender;
        } catch (CorruptedLogException e) {
            IOUtils.closeQuietly(storage);
            throw e;
        }
    }

    private BlockCompressedSegment(Storage storage, Serializer<T> serializer, Codec codec, int maxBlockSize, long maxBlockAddress, int entryIdxBitShift, long position) {
        this.serializer = serializer;
        this.storage = storage;
        this.reader = new FixedBufferDataReader(storage, false, 1);
        this.codec = codec;
        this.maxBlockSize = maxBlockSize;
        this.maxBlockAddress = maxBlockAddress;
        this.currentBlock = new Block(maxBlockSize, position);

        this.entryIdxBitShift = entryIdxBitShift;

        this.maxEntriesPerBlock = (long) Math.pow(2,  entryIdxBitShift);
    }

    @Override
    public long position() {
        return blockStartPosition;
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
        if (currentBlock.size >= maxBlockSize || currentBlock.entries() >= maxEntriesPerBlock) {
            flush();
        }
        ByteBuffer dataBytes = serializer.toBytes(data);
        dataBytes.flip();

        long pos = toBlockPosition(blockStartPosition, currentBlock.data.size());
        currentBlock.add(dataBytes);
        size += dataBytes.limit();
        return pos;
    }

    @Override
    public String name() {
        return storage.name();
    }

    @Override
    public Scanner<T> scanner() {
        return new LogReader(reader, serializer, codec);
    }

    @Override
    public Stream<T> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(scanner(), Spliterator.ORDERED), false);
    }

    @Override
    public Scanner<T> scanner(long position) {
        return new LogReader(reader, serializer, codec, position);
    }

    @Override
    public void close() {
        flush();
        IOUtils.closeQuietly(storage);
    }

    @Override
    public void flush() {
        if(currentBlock.data.isEmpty()) {
            return;
        }
        long nextBlockPosition = currentBlock.writeTo(storage, codec, blockStartPosition);
        currentBlock = new Block(maxBlockSize, nextBlockPosition);//with updated blockStartPosition
        this.blockStartPosition = nextBlockPosition;
    }

    //NOT THREAD SAFE
    private class LogReader extends Scanner<T> {

        private final Codec codec;
        private BlockCompressedSegment.Block currentBlock;
        private int blockEntryIdx;

        private LogReader(DataReader reader, Serializer<T> serializer, Codec codec) {
            this(reader, serializer, codec, 0);
        }

        private LogReader(DataReader reader, Serializer<T> serializer, Codec codec, long initialPosition) {
            super(reader, serializer, initialPosition);
            this.codec = codec;
            this.currentBlock = Block.load(reader, codec, getBlockAddress(initialPosition));
            this.blockEntryIdx = getPositionOnBlock(initialPosition);
            if (this.currentBlock == null) {
                super.completed = true;
            }
        }

        @Override
        protected T readAndVerify() {
            if (blockEntryIdx >= currentBlock.entries()) {
                blockEntryIdx = 0;
                currentBlock = Block.load(reader, codec, currentBlock.nextBlock());
            }
            if (currentBlock == null) {
                return null;
            }
            if (currentBlock.entries() == 0) {
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
        public long position() {
            return toBlockPosition(currentBlock.address, blockEntryIdx);
        }

    }


    private static class Block {

        private static final int READ_ONLY = -1;

        private final List<ByteBuffer> data;
        private int compressedSize = 0;
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
            if (read.limit() == 0) {
                return null;
            }

            int size = read.remaining();
            List<ByteBuffer> entries = decompressing(codec, read);

            Block block = new Block(READ_ONLY, blockAddress, entries);
            block.compressedSize = size;
            block.size = entries.stream().mapToInt(Buffer::limit).sum();
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

        long writeTo(Storage storage, Codec compressor, long position) {
            if (maxSize == READ_ONLY) {
                throw new IllegalStateException("Block is not writable");
            }
            int[] lengths = data.stream().mapToInt(bb -> bb.clear().remaining()).toArray();
            ByteBuffer lengthData = lengthSerializer.toBytes(lengths);

            ByteBuffer withLength = ByteBuffer.allocate(size + lengthData.remaining());
            withLength.put(lengthData); //length + array readFrom int
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
