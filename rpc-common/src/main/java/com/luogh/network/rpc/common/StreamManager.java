package com.luogh.network.rpc.common;

import com.luogh.network.rpc.buffer.ManagedBuffer;
import com.luogh.network.rpc.client.TransportClient;
import io.netty.channel.Channel;

/**
 * @author luogh
 */
public abstract class StreamManager {

    public abstract ManagedBuffer getChunk(long streamId, int chunkIndex);

    public ManagedBuffer openStream(String streamId) {
        throw new UnsupportedOperationException();
    }

    public void registerChannel(Channel channel, long streamId) {}

    public void connectionTerminated(Channel channel) {}

    public void checkAuthorization(TransportClient client, long streamId) {}
}
