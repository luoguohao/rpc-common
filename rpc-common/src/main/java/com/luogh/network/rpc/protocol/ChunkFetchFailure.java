package com.luogh.network.rpc.protocol;

import com.luogh.network.rpc.util.Encoders;
import io.netty.buffer.ByteBuf;
import lombok.Getter;

/**
 * @author luogh
 */
public class ChunkFetchFailure extends AbstractMessage implements ResponseMessage {
    @Getter private final long streamId;
    @Getter private final int chunkIndex;
    @Getter private final String error;

    public ChunkFetchFailure(long streamId, int chunkIndex, String error) {
        this.chunkIndex = chunkIndex;
        this.error = error;
        this.streamId = streamId;
    }

    @Override
    public Type type() {
        return Type.StreamFailure;
    }

    @Override
    public int encodeLength() {
        return 8 + 4 + Encoders.Strings.encodeLength(error);
    }

    @Override
    public void encode(ByteBuf bytebuf) {
        bytebuf.writeInt(chunkIndex);
        bytebuf.writeLong(streamId);
        Encoders.Strings.encode(bytebuf, error);
    }

    public static ChunkFetchFailure decode(ByteBuf byteBuf) {
        int chunkIndex = byteBuf.readInt();
        long streamId = byteBuf.readLong();
        String error = Encoders.Strings.decode(byteBuf);
        return new ChunkFetchFailure(streamId, chunkIndex, error);
    }

    @Override
    public ResponseMessage createFailureMessage(String error) {
        return new ChunkFetchFailure(streamId, chunkIndex, error);
    }
}
