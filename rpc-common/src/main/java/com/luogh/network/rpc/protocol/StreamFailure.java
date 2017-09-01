package com.luogh.network.rpc.protocol;

import com.luogh.network.rpc.util.Encoders;
import io.netty.buffer.ByteBuf;

/**
 * @author luogh
 */
public class StreamFailure extends AbstractMessage implements ResponseMessage {

    private final String streamId;
    private final String error;

    public StreamFailure(String streamId, String error) {
        this.streamId = streamId;
        this.error = error;
    }

    @Override
    public Type type() {
        return Type.StreamFailure;
    }

    @Override
    public int encodeLength() {
        return Encoders.Strings.encodeLength(streamId) + Encoders.Strings.encodeLength(error);
    }

    @Override
    public void encode(ByteBuf bytebuf) {
        Encoders.Strings.encode(bytebuf, streamId);
        Encoders.Strings.encode(bytebuf, error);
    }

    public static StreamFailure decode(ByteBuf byteBuf) {
        String streamId = Encoders.Strings.decode(byteBuf);
        String error = Encoders.Strings.decode(byteBuf);
        return new StreamFailure(streamId, error);
    }

    @Override
    public ResponseMessage createFailureMessage(String error) {
        return new StreamFailure(streamId, error);
    }
}
