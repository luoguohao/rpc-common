package com.luogh.network.rpc.common;

import com.luogh.network.rpc.buffer.ManagedBuffer;

/**
 * @author luogh
 */
public class OneForOneStreamManager extends StreamManager {
    @Override
    public ManagedBuffer getChunk(long streamId, int chunkIndex) {
        return null;
    }
}
