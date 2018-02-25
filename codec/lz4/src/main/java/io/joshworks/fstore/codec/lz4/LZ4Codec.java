package io.joshworks.fstore.codec.lz4;

//public class LZ4Codec implements Codec {
//
//    private final LZ4Factory factory;
//
//    public LZ4Codec() {
//        factory = LZ4Factory.fastestInstance();
//    }
//
//    @Override
//    public byte[] compress(byte[] data) {
//        LZ4Compressor compressor = factory.fastCompressor();
//        return compressor.compress(data);
//    }
//
//    @Override
//    public byte[] decompress(byte[] data, int length) {
//        //FIXME
//        LZ4FastDecompressor decompressor = factory.fastDecompressor();
//        byte[] dst = new byte[length];
//        decompressor.decompress(data, dst);
//        return dst;
//    }
//}
