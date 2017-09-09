package com.luogh.network.rpc.common;

import com.luogh.network.rpc.client.RpcResponseCallback;
import com.luogh.network.rpc.client.TransportClient;

import java.nio.ByteBuffer;

/**
 * @author luogh
 * An RpcHandler suitable for a client-only TransportContext, which can't recieve RPCs.
 */
public class NoOpRpcHandler extends RpcHandler {

    private final StreamManager streamManager;

    public NoOpRpcHandler() {
        this.streamManager = new OneForOneStreamManager();
    }

    @Override
    public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
        throw new UnsupportedOperationException("Can't handle messages.");
    }

    @Override
    public StreamManager streamManager() {
        return streamManager;
    }
}
