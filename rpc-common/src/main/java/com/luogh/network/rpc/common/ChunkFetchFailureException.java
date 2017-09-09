package com.luogh.network.rpc.common;

/**
 * @author luogh
 */
public class ChunkFetchFailureException extends RuntimeException {
    public ChunkFetchFailureException(String error) {
        super(error);
    }
}
