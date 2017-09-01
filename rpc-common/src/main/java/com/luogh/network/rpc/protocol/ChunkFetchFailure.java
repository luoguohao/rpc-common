package com.luogh.network.rpc.protocol;

import com.luogh.network.rpc.util.Encoders;
import io.netty.buffer.ByteBuf;

/**
 * @author luogh
 */
public class ChunkFetchFailure extends AbstractMessage implements ResponseMessage {

    private final int chunkIndex;
    private final String error;

    public ChunkFetchFailure(int chunkIndex, String error) {
        this.chunkIndex = chunkIndex;
        this.error = error;
    }

    @Override
    public Type type() {
        return Type.StreamFailure;
    }

    @Override
    public int encodeLength() {
        return 4 + Encoders.Strings.encodeLength(error);
    }

    @Override
    public void encode(ByteBuf bytebuf) {
        bytebuf.writeInt(chunkIndex);
        Encoders.Strings.encode(bytebuf, error);
    }

    public static ChunkFetchFailure decode(ByteBuf byteBuf) {
        int chunkIndex = byteBuf.readInt();
        String error = Encoders.Strings.decode(byteBuf);
        return new ChunkFetchFailure(chunkIndex, error);
    }

    @Override
    public ResponseMessage createFailureMessage(String error) {
        return new ChunkFetchFailure(chunkIndex, error);
    }
}
