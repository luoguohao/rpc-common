package com.luogh.network.rpc.protocol;

import com.google.common.base.MoreObjects;
import com.luogh.network.rpc.util.Encoders;
import io.netty.buffer.ByteBuf;
import lombok.Getter;

/**
 * @author luogh
 */
@Getter
public class RpcFailureMessage extends AbstractMessage implements ResponseMessage {

    private final long requestId;
    private final String error;

    public RpcFailureMessage(long requestId, String error) {
        super(true, null);
        this.requestId = requestId;
        this.error = error;
    }

    @Override
    public Type type() {
        return Type.RpcFailure;
    }

    @Override
    public int encodeLength() {
        return 8 + Encoders.Strings.encodeLength(error);
    }

    @Override
    public void encode(ByteBuf bytebuf) {
        bytebuf.writeLong(requestId);
        Encoders.Strings.encode(bytebuf,error);
    }

    public static RpcFailureMessage decode(ByteBuf buf) {
        long requestId = buf.readLong();
        String error = Encoders.Strings.decode(buf);
        return new RpcFailureMessage(requestId, error);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("requestId", requestId)
                .add("error", error)
                .toString();
    }

    @Override
    public ResponseMessage createFailureMessage(String error) {
        return new RpcFailureMessage(requestId, error);
    }
}
