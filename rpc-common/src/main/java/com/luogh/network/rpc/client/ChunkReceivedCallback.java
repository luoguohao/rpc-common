package com.luogh.network.rpc.client;

import com.luogh.network.rpc.buffer.ManagedBuffer;

import java.io.IOException; /**
 * @author luogh
 */
public interface ChunkReceivedCallback {

    void onSuccess(int chunkIndex, ManagedBuffer buffer);

    void onFailure(int chunkIndex, Throwable e);
}
