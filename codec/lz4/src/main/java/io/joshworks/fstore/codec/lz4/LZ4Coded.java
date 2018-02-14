package io.joshworks.fstore.codec.lz4;

import io.joshworks.fstore.core.Codec;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

public class LZ4Coded implements Codec {

    private final LZ4Factory factory;

    public LZ4Coded() {
        factory = LZ4Factory.fastestInstance();
    }

    @Override
    public byte[] compress(byte[] data) {
        LZ4Compressor compressor = factory.fastCompressor();
        return compressor.compress(data);
    }

    @Override
    public byte[] decompress(byte[] data, int length) {
        LZ4SafeDecompressor decompressor = factory.safeDecompressor();
        byte[] dst = new byte[length];
        decompressor.decompress(data, dst);
        return dst;
    }
}
