package com.luogh.network.rpc.common;

import com.google.common.base.Preconditions;

import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author luogh
 */
public class LimitedInputStream extends FilterInputStream {

    private final boolean closeWrappedStream;
    private long left;
    private long mark = - 1;


    /**
     * Creates a <code>FilterInputStream</code>
     * by assigning the  argument <code>in</code>
     * to the field <code>this.in</code> so as
     * to remember it for later use.
     *
     * @param in the underlying input stream, or <code>null</code> if
     *           this instance is to be created without an underlying stream.
     */
    public LimitedInputStream(InputStream in, long limit, boolean closeWrappedStream) {
        super(in);
        this.closeWrappedStream = closeWrappedStream;
        Preconditions.checkNotNull(in);
        Preconditions.checkArgument(limit >= 0, "limit must be non-negative.");
        left = limit;
    }

    public LimitedInputStream(InputStream in, long limit) {
        this(in, limit, true);
    }

    @Override
    public int available() throws IOException {
        return (int)Math.min(in.available(), left);
    }

    @Override
    public synchronized void mark(int readLimit) {
        in.mark(readLimit);
        mark = left;
    }

    @Override
    public int read() throws IOException {
        if (left == 0) {
            return -1;
        }
        int result = in.read();
        if (result != -1) {
            --left;
        }
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (left == 0) {
            return -1;
        }
        len = (int)Math.min(len, left);
        int result = in.read(b, off, len);
        if (result != -1) {
            left -= result;
        }
        return result;
    }

    @Override
    public synchronized void reset() throws IOException {
        if (!in.markSupported()) {
            throw new IOException("Mark not supported.");
        }
        if (mark == -1) {
            throw new IOException("Mark not set");
        }
        in.reset();
        left = mark;
    }

    @Override
    public long skip(long n) throws IOException {
        n = Math.min(n, left);
        long skip = in.skip(n);
        left -= skip;
        return skip;
    }

    @Override
    public void close() throws IOException {
        if (closeWrappedStream) {
            super.close();
        }
    }
}
