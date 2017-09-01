package com.luogh.network.rpc.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author luogh
 */
public class NioManagedBuffer implements ManagedBuffer {

    private final ByteBuffer buf;

    public NioManagedBuffer(ByteBuffer buf) {
        this.buf = buf;
    }

    @Override
    public long size() {
        return buf.remaining();
    }

    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
        return buf;
    }

    @Override
    public InputStream createInputStream() throws IOException {
        return new ByteBufInputStream(Unpooled.wrappedBuffer(buf));
    }

    @Override
    public ManagedBuffer retain() {
        return this;
    }

    @Override
    public ManagedBuffer release() {
        return this;
    }

    @Override
    public Object convertToNetty() throws IOException {
        return Unpooled.wrappedBuffer(buf);
    }
}
