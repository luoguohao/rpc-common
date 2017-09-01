package com.luogh.network.rpc.common;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author luogh
 */
@AllArgsConstructor
@Getter
public class StreamChunkId {
    private final String streamId;
    private final int chunkIndex;
}
