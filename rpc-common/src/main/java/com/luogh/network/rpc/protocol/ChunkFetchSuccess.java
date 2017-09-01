package com.luogh.network.rpc.protocol;

import com.luogh.network.rpc.buffer.ManagedBuffer;
import com.luogh.network.rpc.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;

/**
 * @author luogh
 */
public class ChunkFetchSuccess extends AbstractMessage implements ResponseMessage {

    private final int chunkIndex;

    public ChunkFetchSuccess(int chunkIndex, ManagedBuffer body) {
        super(true, body);
        this.chunkIndex = chunkIndex;
    }


    @Override
    public Type type() {
        return Type.ChunkFetchSuccess;
    }

    @Override
    public int encodeLength() {
        return 4;
    }

    @Override
    public void encode(ByteBuf bytebuf) {
        bytebuf.writeInt(chunkIndex);
    }

    public static ChunkFetchSuccess decode(ByteBuf byteBuf) {
        int chunkIndex = byteBuf.readInt();
        return new ChunkFetchSuccess(chunkIndex, new NettyManagedBuffer(byteBuf.retainedDuplicate()));
    }

    @Override
    public ResponseMessage createFailureMessage(String error) {
        return new ChunkFetchFailure(chunkIndex, error);
    }
}
