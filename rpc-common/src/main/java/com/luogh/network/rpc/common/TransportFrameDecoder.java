package com.luogh.network.rpc.common;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.LinkedList;

/**
 * @author luogh
 */
public class TransportFrameDecoder extends ChannelInboundHandlerAdapter {

    private static final int FRAME_LENGTH_SIZE = 8;
    private static final int MAX_FRAME_LENGTH_SIZE = Integer.MAX_VALUE;
    private static final int UNKNOWN_FRAME_LENGTH_SIZE = -1;

    private final LinkedList<ByteBuf> buffers = new LinkedList<>();
    private final ByteBuf frameLenBuf = Unpooled.buffer(FRAME_LENGTH_SIZE, FRAME_LENGTH_SIZE);

    private long totalSize = 0;
    private long nextFrameSize = UNKNOWN_FRAME_LENGTH_SIZE;
    private volatile Interceptor interceptor;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf in = (ByteBuf)msg;
        buffers.add(in);
        totalSize += in.readableBytes();

        while (!buffers.isEmpty()) {
            if (interceptor != null) {
                ByteBuf first = buffers.getFirst();
                int available = first.readableBytes();

                if (feedInterceptor(first)) {
                    assert !first.isReadable() : "Interceptor still active but buffer has data.";
                }
                int read = available - first.readableBytes();
                if (read == available) {
                    buffers.removeFirst().release();
                }
                totalSize -= read;
            } else {
                // Interceptor is not active, so try to decode a frame.
                ByteBuf frame = decodeNext();
                if (frame == null) {
                    break;
                }
                ctx.fireChannelRead(frame);
            }
        }

    }

    private ByteBuf decodeNext() {
        long frameSize = decodeFrameSize();
        if (frameSize == UNKNOWN_FRAME_LENGTH_SIZE || totalSize < frameSize) { // total buffer size not contain all frame data.
            return null;
        }
        //Resize for the next frameSize
        nextFrameSize = UNKNOWN_FRAME_LENGTH_SIZE;

        Preconditions.checkArgument(frameSize < MAX_FRAME_LENGTH_SIZE, "Too large frame: %s.", frameSize);
        Preconditions.checkArgument(frameSize > 0, "Frame size should be positive: %s.", frameSize);

        int remaining = (int)frameSize;
        if (buffers.getFirst().readableBytes() > remaining) { // if first buffer contain all frame data, return it.
            return nextBufferForFrame(remaining);
        }
        // otherwise, create a composite buffer holds the entire frame, return it.
        CompositeByteBuf frame = buffers.getFirst().alloc().compositeBuffer(Integer.MAX_VALUE);
        while (remaining > 0) {
            ByteBuf next = nextBufferForFrame(remaining);
            remaining -= next.readableBytes();
            frame.addComponent(next).writerIndex(frame.writerIndex() + next.readableBytes());
        }
        assert remaining == 0;
        return frame;
    }

    private ByteBuf nextBufferForFrame(int remaining) {
        ByteBuf buf = buffers.getFirst();
        ByteBuf frame;
        if (buf.readableBytes() > remaining) {
            frame = buf.retain().readSlice(remaining);
            totalSize -= frame.readableBytes();
        } else {
            frame = buf;
            buffers.removeFirst();
            totalSize -= buf.readableBytes();
        }
        return frame;
    }

    private long decodeFrameSize() {
        if (nextFrameSize != UNKNOWN_FRAME_LENGTH_SIZE || totalSize < FRAME_LENGTH_SIZE) {
            return nextFrameSize;
        }

        ByteBuf first = buffers.getFirst();
        if (first.readableBytes() > FRAME_LENGTH_SIZE) {
            nextFrameSize = first.readLong() - FRAME_LENGTH_SIZE;
            totalSize -= FRAME_LENGTH_SIZE;
            if (!first.isReadable()) {
                buffers.removeFirst().release();
            }
            return nextFrameSize;
        }

        while (frameLenBuf.readableBytes() < FRAME_LENGTH_SIZE) {
            ByteBuf buf = buffers.getFirst();
            int toRead = Math.min(buf.readableBytes(), FRAME_LENGTH_SIZE - frameLenBuf.readableBytes());
            frameLenBuf.writeBytes(buf, toRead);
            if (!buf.isReadable()) {
                buffers.removeFirst().release();
            }
        }
        nextFrameSize = frameLenBuf.readLong() - FRAME_LENGTH_SIZE;
        totalSize -= FRAME_LENGTH_SIZE;
        frameLenBuf.clear();
        return nextFrameSize;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        for (ByteBuf byteBuf: buffers) {
            byteBuf.release();
        }
        if (interceptor != null) {
            interceptor.channelInactive();
        }
        frameLenBuf.release();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (interceptor != null) {
            interceptor.exceptionCaught(cause);
        }
        super.exceptionCaught(ctx, cause);
    }

    /**
     * Handle receive data
     * @param data
     * @return if true ,interceptor still need data, false ,uninstall it.
     * @throws Exception
     */
    private boolean feedInterceptor(ByteBuf data) throws Exception {
        if (interceptor != null && !interceptor.handle(data)) {
            interceptor = null;
        }
        return interceptor != null;
    }

    public interface Interceptor {

        /**
         * Handles data received from the remote end.
         * @param data Buffer contains data.
         * @return true if the interceptor expect more data, false to install the interceptor.
         * @throws Exception
         */
        boolean handle(ByteBuf data) throws Exception;

        /**
         * Called if an exception is thrown in the channel pipeline.
         * @param cause
         * @throws Exception
         */
        void exceptionCaught(Throwable cause) throws Exception;

        /**
         * called if channel is closed and the interceptor is still installed.
         * @throws Exception
         */
        void channelInactive() throws Exception;
    }
}
