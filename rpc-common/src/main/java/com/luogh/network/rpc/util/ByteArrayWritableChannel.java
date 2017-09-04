package com.luogh.network.rpc.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * @author luogh
 */
public class ByteArrayWritableChannel implements WritableByteChannel {

    private final byte[] data;
    private int offset;

    public ByteArrayWritableChannel(int size) {
        this.data = new byte[size];
    }

    public byte[] getData() {
        return this.data;
    }

    public int getLength() {
        return this.offset;
    }

    public void reset() {
        this.offset = 0;
    }


    @Override
    public int write(ByteBuffer src) throws IOException {
        int toTransfer = Math.min(src.remaining(), data.length - offset);
        src.get(data, offset, toTransfer);
        offset += toTransfer;
        return toTransfer;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void close() throws IOException {

    }
}
