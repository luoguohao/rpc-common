package com.luogh.network.rpc.protocol;

import com.luogh.network.rpc.buffer.ManagedBuffer;
import com.luogh.network.rpc.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;
import lombok.Getter;

/**
 * @author luogh
 */
public class ChunkFetchSuccess extends AbstractMessage implements ResponseMessage {

    @Getter private final long streamId;
    @Getter private final int chunkIndex;

    public ChunkFetchSuccess(long streamId, int chunkIndex, ManagedBuffer body) {
        super(true, body);
        this.chunkIndex = chunkIndex;
        this.streamId = streamId;
    }


    @Override
    public Type type() {
        return Type.ChunkFetchSuccess;
    }

    @Override
    public int encodeLength() {
        return 8 + 4;
    }

    @Override
    public void encode(ByteBuf bytebuf) {
        bytebuf.writeInt(chunkIndex);
        bytebuf.writeLong(streamId);
    }

    public static ChunkFetchSuccess decode(ByteBuf byteBuf) {
        int chunkIndex = byteBuf.readInt();
        long streamId = byteBuf.readLong();
        return new ChunkFetchSuccess(streamId, chunkIndex, new NettyManagedBuffer(byteBuf.retainedDuplicate()));
    }

    @Override
    public ResponseMessage createFailureMessage(String error) {
        return new ChunkFetchFailure(streamId, chunkIndex, error);
    }
}
