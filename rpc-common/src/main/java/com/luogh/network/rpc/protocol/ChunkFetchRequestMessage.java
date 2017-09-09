package com.luogh.network.rpc.protocol;

import com.luogh.network.rpc.util.Encoders;
import io.netty.buffer.ByteBuf;
import lombok.Getter;

import java.nio.ByteBuffer;

/**
 * @author luogh
 */
public class ChunkFetchRequestMessage extends AbstractMessage implements RequestMessage {

    @Getter private final long streamId;
    @Getter private final int chunkIndex;

    public ChunkFetchRequestMessage(long streamId, int chunkIndex) {
        this.streamId = streamId;
        this.chunkIndex = chunkIndex;
    }

    @Override
    public Type type() {
        return Type.ChunkFetchRequest;
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

    public static ChunkFetchRequestMessage decode(ByteBuf byteBuffer) {
        int chunkIndex = byteBuffer.readInt();
        long streamId = byteBuffer.readLong();
        return new ChunkFetchRequestMessage(streamId, chunkIndex);
    }
}
