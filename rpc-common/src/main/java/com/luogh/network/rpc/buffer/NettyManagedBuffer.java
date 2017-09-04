package com.luogh.network.rpc.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author luogh
 */
public class NettyManagedBuffer implements ManagedBuffer {

    private final ByteBuf buf;

    public NettyManagedBuffer(ByteBuf buf) {
        this.buf = buf;
    }

    @Override
    public long size() {
        return buf.readableBytes();
    }

    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
        return buf.nioBuffer();
    }

    @Override
    public InputStream createInputStream() throws IOException {
        return new ByteBufInputStream(buf);
    }

    @Override
    public ManagedBuffer retain() {
        buf.retain();
        return this;
    }

    @Override
    public ManagedBuffer release() {
        buf.release();
        return this;
    }

    @Override
    public Object convertToNetty() throws IOException {
        return buf.duplicate().retain();
    }


}
