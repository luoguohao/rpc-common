package com.luogh.network.rpc.protocol;

import com.google.common.base.Preconditions;
import com.luogh.network.rpc.buffer.ManagedBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * @author luogh
 */

/**
 * A wrapper message that holds two separate pieces (a header and a body).
 * The header must be a ByteBuf, while the body can be a ByteBuf or FileRegion.
 */
public class MessageWithHeader extends AbstractReferenceCounted implements FileRegion {

    private final ManagedBuffer body;
    private final ByteBuf header;
    private final Object bodyObj;
    private final long bodyLength;
    private final int headerLength;
    private long alreadyTransformedBytes;

    /**
     * When the write buffer size is larger than this limit, I/O will be done in chunks of this size.
     * The size should not be too large as it will waste underlying memory copy. e.g. If network
     * avaliable buffer is smaller than this limit, the data cannot be sent within one single write
     * operation while it still will make memory copy with this size.
     */
    private static final int NIO_BUFFER_LIMIT = 256 * 1024;

    public MessageWithHeader(ManagedBuffer body, ByteBuf header, Object bodyObj, long bodyLength) {
        this.body = body;
        this.header = header;
        this.headerLength = header.readableBytes();
        this.bodyObj = bodyObj;
        this.bodyLength = bodyLength;
    }

    @Override
    public MessageWithHeader retain() {
        super.retain();
        return this;
    }

    @Override
    public MessageWithHeader retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public MessageWithHeader touch() {
        return this;
    }

    @Override
    public MessageWithHeader touch(Object hint) {
        return this;
    }

    @Override
    public long position() {
        return 0;
    }

    @Override
    public long transfered() {
        return transferred();
    }

    @Override
    public long transferred() {
        return alreadyTransformedBytes;
    }

    @Override
    public long count() {
        return headerLength + bodyLength;
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        Preconditions.checkArgument(position == alreadyTransformedBytes, "Invalid position.");

        // write header out
        long writtenHeader = 0;
        if (header.readableBytes() > 0) {
            writtenHeader = copyByteBuf(header, target);
            alreadyTransformedBytes += writtenHeader;
            if (header.readableBytes() > 0) {
                return writtenHeader;
            }
        }

        // reached here means header has already consumed all.
        long writtenBody = 0;
        if (bodyObj instanceof FileRegion) {
            writtenBody = ((FileRegion) bodyObj).transferTo(target, alreadyTransformedBytes - headerLength);
        } else if (bodyObj instanceof ByteBuf){
            writtenBody = copyByteBuf((ByteBuf)bodyObj, target);
        }
        alreadyTransformedBytes += writtenBody;
        return writtenHeader + writtenBody;
    }

    private long copyByteBuf(ByteBuf byteBuf, WritableByteChannel target) throws IOException {
        ByteBuffer buffer = byteBuf.nioBuffer();
        int written = buffer.remaining() <= NIO_BUFFER_LIMIT ?
                target.write(buffer) : writeNioBuffer(target, buffer);
        byteBuf.skipBytes(written);
        return 0;
    }

    private int writeNioBuffer(WritableByteChannel target, ByteBuffer buffer) {
        int originLimit = buffer.limit();
        int written = 0;

        try {
            int curLimit = Math.min(buffer.remaining(), NIO_BUFFER_LIMIT);
            buffer.limit(buffer.position() + curLimit);
            written = target.write(buffer);
        } catch (IOException e) {
            buffer.limit(originLimit);
        }
        return written;
    }

    @Override
    protected void deallocate() {
        if (header != null) {
            header.release();
        }
        body.release();
        if (bodyObj != null) {
            ReferenceCountUtil.release(bodyObj);
        }

    }
}
