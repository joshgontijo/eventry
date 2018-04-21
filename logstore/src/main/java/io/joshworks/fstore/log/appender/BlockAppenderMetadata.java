//package io.joshworks.fstore.log.appender;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//
//public class BlockAppenderMetadata  {
//
//    final Metadata base;
//    final int maxBlockSize;
//    final int entryIdxBitShift;
//
//    public BlockAppenderMetadata(Metadata base, int maxBlockSize, int entryIdxBitShift) {
//        this.base = base;
//        this.maxBlockSize = maxBlockSize;
//        this.entryIdxBitShift = entryIdxBitShift;
//    }
//
//    public static <T> BlockAppenderMetadata of(BlockSegmentBuilder<T> builder) {
//        Metadata base = Metadata.readFrom(builder.base);
//        return new BlockAppenderMetadata(base, builder.maxBlockSize, builder.entryIdxBitShift);
//    }
//
//    public static BlockAppenderMetadata of(DataInput in) throws IOException {
//        Metadata base = Metadata.readFrom(in);
//        int blockSize = in.readInt();
//        int entryIdxBitShift = in.readInt();
//        return new BlockAppenderMetadata(base, blockSize, entryIdxBitShift);
//    }
//
//    public void writeTo(DataOutput out) throws IOException {
//        base.writeTo(out);
//        out.writeInt(maxBlockSize);
//        out.writeInt(entryIdxBitShift);
//    }
//
//}
