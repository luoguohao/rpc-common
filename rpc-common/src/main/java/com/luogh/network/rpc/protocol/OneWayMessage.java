package com.luogh.network.rpc.protocol;

import com.luogh.network.rpc.buffer.ManagedBuffer;
import com.luogh.network.rpc.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * @author luogh
 */
public class OneWayMessage extends AbstractMessage implements RequestMessage {

    public OneWayMessage(ManagedBuffer body) {
        super(true, body);
    }

    @Override
    public Type type() {
        return Type.OneWayMessage;
    }

    @Override
    public int encodeLength() {
        return 4;
    }

    @Override
    public void encode(ByteBuf bytebuf) {
        bytebuf.writeInt((int)body().size());
    }

    public static OneWayMessage decode(ByteBuf byteBuf) {
        byteBuf.readInt();
        return new OneWayMessage(new NettyManagedBuffer(byteBuf.retain()));
    }
}
