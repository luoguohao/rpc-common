package com.luogh.network.rpc.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author luogh
 */
public interface ManagedBuffer {

    /** Number of bytes for buffer. **/
    long size();

    ByteBuffer nioByteBuffer() throws IOException;

    InputStream createInputStream() throws IOException;

    /** Increment the reference count if applicable. **/
    ManagedBuffer retain();

    /**
     *  If applicable, decrease the reference count by one ,
     *  and deallocates the buffer if the reference count
     *  reaches zero.
     */
    ManagedBuffer release();

    /**
     * Convert the buffer to a Netty Object, used to write data
     * out,the return value is ether a {@link io.netty.buffer.ByteBuf}
     * or a {@link io.netty.channel.FileRegion}.
     * @return
     * @throws IOException
     */
    Object convertToNetty() throws IOException;
}
