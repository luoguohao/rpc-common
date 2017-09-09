package com.luogh.network.rpc.common;

import com.luogh.network.rpc.client.StreamReceivedCallback;
import com.luogh.network.rpc.client.TransportResponseHandler;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

/**
 * @author luogh
 */
public class StreamInterceptor implements TransportFrameDecoder.Interceptor {
    private final TransportResponseHandler responseHandler;
    private final String streamId;
    private final long byteCount;
    private final StreamReceivedCallback callback;

    private long bytesRead;

    public StreamInterceptor(TransportResponseHandler transportResponseHandler, String streamId,
                             long byteCount, StreamReceivedCallback callback) {
        this.responseHandler = transportResponseHandler;
        this.streamId = streamId;
        this.byteCount = byteCount;
        this.callback = callback;
        this.bytesRead = 0;
    }

    /**
     * FrameDecoder will call this method multi-times until the method return false.
     * @param data Buffer contains data.
     * @return Return true means the Interceptor needs more data.
     * @throws Exception
     */
    @Override
    public boolean handle(ByteBuf data) throws Exception {
        int toRead = (int)Math.min(data.readableBytes(), byteCount - bytesRead);
        ByteBuffer nioBuffer = data.readSlice(toRead).nioBuffer();
        int available  = nioBuffer.remaining();
        callback.onDataReceived(streamId, nioBuffer);
        bytesRead += available;
        if (bytesRead > byteCount) {
            RuntimeException e = new IllegalStateException(String.format(
                    "Read too many bytes? Expected %d, but read %d.", byteCount, bytesRead
            ));
            callback.onFailure(streamId, e);
            responseHandler.deactivateStream();
            throw e;
        } else if (byteCount == bytesRead){
            responseHandler.deactivateStream();
            callback.onComplete(streamId);
        }
        return byteCount != bytesRead;
    }

    @Override
    public void exceptionCaught(Throwable cause) throws Exception {
        responseHandler.deactivateStream();
        callback.onFailure(streamId, cause);
    }

    @Override
    public void channelInactive() throws Exception {
        responseHandler.deactivateStream();
        callback.onFailure(streamId, new ClosedChannelException());
    }
}
