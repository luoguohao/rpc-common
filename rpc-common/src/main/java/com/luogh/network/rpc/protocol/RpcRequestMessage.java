package com.luogh.network.rpc.protocol;

import com.google.common.base.MoreObjects;
import com.luogh.network.rpc.buffer.ManagedBuffer;
import com.luogh.network.rpc.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;
import lombok.Getter;

import java.util.Objects;


/**
 * @author luogh
 */
public final class RpcRequestMessage extends AbstractMessage implements RequestMessage {

    @Getter
    private final long requestId;

    public RpcRequestMessage(ManagedBuffer body, long requestId) {
        super(true, body);
        this.requestId = requestId;
    }


    @Override
    public Type type() {
        return Type.RpcRequest;
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

    public static RpcRequestMessage decode(ByteBuf byteBuf) {
        long requestId = byteBuf.readLong();
        byteBuf.readInt();
        return new RpcRequestMessage(new NettyManagedBuffer(byteBuf.retain()), requestId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, body());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RpcRequestMessage) {
            RpcRequestMessage o = (RpcRequestMessage) obj;
            return requestId == o.requestId && super.equals(o);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("requestId", requestId)
                .add("body", body())
                .toString();
    }
}
