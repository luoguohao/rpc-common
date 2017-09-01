package com.luogh.network.rpc.protocol;

import com.luogh.network.rpc.buffer.ManagedBuffer;
import com.luogh.network.rpc.buffer.NettyManagedBuffer;
import com.luogh.network.rpc.util.Encoders;
import io.netty.buffer.ByteBuf;

/**
 * @author luogh
 */
public class StreamResponse extends AbstractMessage implements ResponseMessage {

    private final String streamId;
    private final long byteCount;

    public StreamResponse(String streamId, long byteCount, ManagedBuffer body) {
        super(false, body);
        this.streamId = streamId;
        this.byteCount = byteCount;
    }

    @Override
    public Type type() {
        return Type.StreamResponse;
    }

    @Override
    public int encodeLength() {
        return 8 + Encoders.Strings.encodeLength(streamId);
    }

    @Override
    public void encode(ByteBuf bytebuf) {
        bytebuf.writeLong(byteCount);
        Encoders.Strings.encode(bytebuf, streamId);
    }

    public static StreamResponse decode(ByteBuf byteBuf) {
        long byteCount = byteBuf.readLong();
        String streamId = Encoders.Strings.decode(byteBuf);
        return new StreamResponse(streamId, byteCount, null);
    }

    @Override
    public ResponseMessage createFailureMessage(String error) {
        return new StreamFailure(streamId, error);
    }
}
