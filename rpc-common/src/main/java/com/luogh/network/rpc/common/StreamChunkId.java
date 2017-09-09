package com.luogh.network.rpc.common;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * @author luogh
 */
@AllArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public class StreamChunkId {
    private final long streamId;
    private final int chunkIndex;
}
