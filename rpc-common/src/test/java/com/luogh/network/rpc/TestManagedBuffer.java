package com.luogh.network.rpc;

import com.google.common.base.Preconditions;
import com.luogh.network.rpc.buffer.ManagedBuffer;
import com.luogh.network.rpc.buffer.NettyManagedBuffer;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author luogh
 */
@Slf4j
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

    @Override
    public boolean equals(Object o) {
        if (o instanceof ManagedBuffer) {
            try {
                ByteBuffer nioBuf = ((ManagedBuffer) o).nioByteBuffer();
                if (nioBuf.remaining() != len) {
                    log.error("nioBuffer remaining {} != length {} ", nioBuf.remaining(), len);
                    return false;
                } else {
                    for (int i = 0; i < len; i++) {
                        if (nioBuf.get(i) != i) {
                            log.error("nio buf.get({}) != {}", i, i);
                            return false;
                        }
                    }
                    return true;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        log.error("not a managedBuffer.");
        return false;
    }

    @Override
    public int hashCode() {
      return underlying.hashCode();
    }
}
