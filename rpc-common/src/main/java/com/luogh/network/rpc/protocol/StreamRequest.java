package com.luogh.network.rpc.protocol;

import com.luogh.network.rpc.util.Encoders;
import io.netty.buffer.ByteBuf;
import lombok.Getter;

/**
 * @author luogh
 */
public class StreamRequest extends AbstractMessage implements RequestMessage {

    @Getter private final String streamId;

    public StreamRequest(String streamId) {
        this.streamId = streamId;
    }

    @Override
    public Type type() {
        return Type.StreamRequest;
    }

    @Override
    public int encodeLength() {
        return Encoders.Strings.encodeLength(streamId);
    }

    @Override
    public void encode(ByteBuf bytebuf) {
        Encoders.Strings.encode(bytebuf, streamId);
    }

    public static StreamRequest decode(ByteBuf byteBuf) {
        return new StreamRequest(Encoders.Strings.decode(byteBuf));
    }
}
