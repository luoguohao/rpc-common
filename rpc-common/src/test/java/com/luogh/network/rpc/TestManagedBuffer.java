package com.luogh.network.rpc;

import com.google.common.base.Preconditions;
import com.luogh.network.rpc.buffer.ManagedBuffer;
import com.luogh.network.rpc.buffer.NettyManagedBuffer;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author luogh
 */
public class TestManagedBuffer implements ManagedBuffer {

    private final int len;
    private NettyManagedBuffer underlying;

    public TestManagedBuffer(int len) {
        Preconditions.checkArgument(len <= Byte.MAX_VALUE);
        this.len = len;
        byte[] byteArray = new byte[len];
        for (int i=0; i < len; i++) {
            byteArray[i] = (byte)i;
        }
        this.underlying = new NettyManagedBuffer(Unpooled.wrappedBuffer(byteArray));
    }

    @Override
    public long size() {
        return underlying.size();
    }

    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
        return underlying.nioByteBuffer();
    }

    @Override
    public InputStream createInputStream() throws IOException {
        return underlying.createInputStream();
    }

    @Override
    public ManagedBuffer retain() {
        return underlying.retain();
    }

    @Override
    public ManagedBuffer release() {
        return underlying.release();
    }

    @Override
    public Object convertToNetty() throws IOException {
        return underlying.convertToNetty();
    }
}
