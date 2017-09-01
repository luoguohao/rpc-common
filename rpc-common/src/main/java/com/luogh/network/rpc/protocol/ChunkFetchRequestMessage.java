package com.luogh.network.rpc.protocol;

import com.luogh.network.rpc.util.Encoders;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * @author luogh
 */
public class ChunkFetchRequestMessage extends AbstractMessage implements RequestMessage {

    private final String streamId;
    private final int chunkIndex;

    public ChunkFetchRequestMessage(String streamId, int chunkIndex) {
        this.streamId = streamId;
        this.chunkIndex = chunkIndex;
    }

    @Override
    public Type type() {
        return Type.ChunkFetchRequest;
    }

    @Override
    public int encodeLength() {
        return Encoders.Strings.encodeLength(streamId) + 4;
    }

    @Override
    public void encode(ByteBuf bytebuf) {
        bytebuf.writeInt(chunkIndex);
        Encoders.Strings.encode(bytebuf, streamId);
    }

    public static ChunkFetchRequestMessage decode(ByteBuf byteBuffer) {
        int chunkIndex = byteBuffer.readInt();
        String streamId = Encoders.Strings.decode(byteBuffer);
        return new ChunkFetchRequestMessage(streamId, chunkIndex);
    }
}
