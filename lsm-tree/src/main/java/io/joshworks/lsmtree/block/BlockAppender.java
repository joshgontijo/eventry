package io.joshworks.lsmtree.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.serializer.DirectSerializer;

import java.io.File;
import java.nio.ByteBuffer;

public class BlockAppender<T> {

    private final LogAppender<ByteBuffer> appender;
    private Block<T> block;
    private final Codec codec;

    public BlockAppender(File directory, Codec codec) {
        this.appender = LogAppender.simpleLog(new Builder<>(directory, new DirectSerializer()));
        this.codec = codec;
    }

    public void add(T data) {
        //here is assumed that the compressed size will be less than the max block size,
        //if not an additional read will occur when querying the data
        boolean aboveSizeThreshold = block.add(data);
        if(aboveSizeThreshold) {
            ByteBuffer compressed = block.compress(codec);
            T firstEntry = block.first();
            long address = appender.append(compressed);


        }
    }




}
