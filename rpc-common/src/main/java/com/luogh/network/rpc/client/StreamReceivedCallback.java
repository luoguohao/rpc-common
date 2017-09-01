package com.luogh.network.rpc.client;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author luogh
 */
public interface StreamReceivedCallback {
    void onDataReceived(String streamId, ByteBuffer data) throws IOException;

    void onComplete(String streamId) throws IOException;

    void onFailure(String streamId, Throwable e) throws IOException;
}
