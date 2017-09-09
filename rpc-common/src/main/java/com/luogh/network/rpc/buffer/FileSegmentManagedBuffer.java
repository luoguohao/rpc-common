package com.luogh.network.rpc.buffer;

import com.google.common.io.ByteStreams;
import com.luogh.network.rpc.common.LimitedInputStream;
import com.luogh.network.rpc.common.TransportConf;
import io.netty.channel.DefaultFileRegion;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author luogh
 */
@AllArgsConstructor
@Getter
public class FileSegmentManagedBuffer implements ManagedBuffer {

    private final TransportConf conf;
    private final File file;
    private final long offset;
    private final long length;

    @Override
    public long size() {
        return length;
    }

    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
        FileChannel channel = null;
        try {
            channel = new RandomAccessFile(file, "r").getChannel();
            //just copy the buffer if it is sufficiently small, as memory mapping has a high overhead.
            if (length < conf.memoryMapBytes()) {
                ByteBuffer buf = ByteBuffer.allocate((int)length);
                channel.position(offset);
                while (buf.remaining() != 0) {
                    if (channel.read(buf) == -1) {
                        throw new IOException(String.format("Reached EOF before filling buffer. %n"
                        + "offset=%s %n file=%s %n buffer.remaining=%s", offset, file, buf.remaining()));
                    }
                }
                buf.flip();
                return buf;
            } else {
                return channel.map(FileChannel.MapMode.READ_ONLY, offset, length);
            }
        } catch (IOException e) {
            if (channel != null) {
                long size = channel.size();
                throw new IOException("Error in reading "+ this + "(actual file length " + size + ")");
            }
            throw new IOException("Error in opening " + this, e);
        } finally {
            if (channel != null) {
                channel.close();
            }
        }
    }

    @Override
    public InputStream createInputStream() throws IOException {
        FileInputStream stream = null;
        try {
            stream = new FileInputStream(file);
            ByteStreams.skipFully(stream, offset);
            return new LimitedInputStream(stream, length);
        } catch (Exception e) {
            if (stream != null) {
                long size = file.length();
                throw new IOException("Error in reading " + this + " (actual file length " + size + " )");
            }
            throw new IOException("Error in opening " + this, e);
        } finally {
            if (stream != null) {
                stream.close();
            }
        }
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
        if (conf.lazyFileDescriptor()) {
            return new DefaultFileRegion(file, offset, length);
        } else {
            return new DefaultFileRegion(new FileInputStream(file).getChannel(), offset, length);
        }
    }
}
