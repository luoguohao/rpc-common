package com.luogh.network.rpc.protocol;

import com.google.common.base.MoreObjects;
import com.luogh.network.rpc.buffer.ManagedBuffer;
import com.luogh.network.rpc.buffer.NettyManagedBuffer;
import com.luogh.network.rpc.buffer.NioManagedBuffer;
import io.netty.buffer.ByteBuf;
import lombok.Getter;

/**
 * @author luogh
 */
public class RpcResponseMessage extends AbstractMessage implements ResponseMessage {

    @Getter
    private final long requestId;

    public RpcResponseMessage(ManagedBuffer body, long requestId) {
        super(true, body);
        this.requestId = requestId;
    }

    @Override
    public Type type() {
        return Type.RpcResponse;
    }

    @Override
    public int encodeLength() {
        return 8 + 4;
    }

    @Override
    public void encode(ByteBuf bytebuf) {
        bytebuf.writeLong(requestId);
        bytebuf.writeInt((int)body().size());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("requestId", requestId)
                .add("body", body())
                .toString();
    }

    public static RpcResponseMessage decode(ByteBuf msg) {
        long requestId = msg.readLong();
        return new RpcResponseMessage(new NettyManagedBuffer(msg.retain()), requestId);
    }

    @Override
    public ResponseMessage createFailureMessage(String error) {
        return new RpcFailureMessage(requestId, error);
    }
}
