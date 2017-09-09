package com.luogh.network.rpc.common;

import com.google.common.base.Preconditions;
import com.luogh.network.rpc.buffer.ManagedBuffer;
import com.luogh.network.rpc.client.TransportClient;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * StreamManger which allows registration of an Iterator<ManagedBuffer>
 * ; which individually fetched as chunks by the client. Each registration
 * buffer is on chunk.
 * @author luogh
 */
@Slf4j
public class OneForOneStreamManager extends StreamManager {
    private final AtomicLong nextStreamId;
    private final ConcurrentHashMap<Long, StreamState> streams;

    public OneForOneStreamManager() {
        this.nextStreamId = new AtomicLong((long) new Random().nextInt(Integer.MAX_VALUE) * 1000);
        streams = new ConcurrentHashMap<>();
    }

    /**
     * State of a single stream.
     */
    private static class StreamState {
        final String appId;
        final Iterator<ManagedBuffer> buffers;

        // The channel associated to the stream
        Channel associatedChannel = null;

        // Used to keep track of the index of the buffer that the user has retrieved,just to ensure that
        // the caller only requests each chunk one at a time, in order
        int curChunk = 0;

        StreamState (String appId, Iterator<ManagedBuffer> buffers) {
            this.appId = appId;
            this.buffers = Preconditions.checkNotNull(buffers);
        }
    }

    public long registerStream(String appId, Iterator<ManagedBuffer> buffers) {
        long streamId = nextStreamId.incrementAndGet();
        streams.put(streamId, new StreamState(appId, buffers));
        return streamId;
    }

    @Override
    public void registerChannel(Channel channel, long streamId) {
        if (streams.contains(streamId)) {
            streams.get(streamId).associatedChannel = channel;
        }
    }

    @Override
    public void connectionTerminated(Channel channel) {
        for (Map.Entry<Long, StreamState> entry : streams.entrySet()) {
            StreamState state = entry.getValue();
            if (state.associatedChannel == channel ) {
                streams.remove(entry.getKey());
                // release all the remaining buffers.
                while (state.buffers.hasNext()) {
                    state.buffers.next().release();
                }
            }

        }
    }

    @Override
    public void checkAuthorization(TransportClient client, long streamId) {
        if (client.getClientId() != null) {
            StreamState state = streams.get(streamId);
            Preconditions.checkArgument(state != null, "Unknown streamId.");
            if (!client.getClientId().equals(state.appId)) {
                throw new SecurityException(String.format(
                        "Client %s not authorized to read stream %d(app %s).",
                        client.getClientId(), streamId, state.appId
                ));
            }
        }
    }

    @Override
    public ManagedBuffer getChunk(long streamId, int chunkIndex) {
        StreamState state = streams.get(streamId);
        Preconditions.checkArgument(state != null, "Unknown streamId");
        if (state.curChunk != chunkIndex) {
            throw new IllegalStateException(String.format(
                    "Received out-of-order chunk index %d (expected %d)",chunkIndex, state.curChunk
            ));
        } else if (!state.buffers.hasNext()) {
            throw new IllegalStateException(String.format(
                    "Requested chunk index beyond end %s", chunkIndex)
            );
        }
        state.curChunk += 1;
        ManagedBuffer buf = state.buffers.next();
        if (!state.buffers.hasNext()) {
            log.trace("Removing stream id {}.", streamId);
            streams.remove(streamId);
        }
        return buf;
    }
}
