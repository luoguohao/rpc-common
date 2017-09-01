package com.luogh.network.rpc.common;

import com.luogh.network.rpc.client.RpcResponseCallback;
import com.luogh.network.rpc.client.TransportClient;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

/**
 * @author luogh
 */
@Slf4j
public abstract class RpcHandler {

    private static final RpcResponseCallback ONE_WAY_CALLBACK = new OneWayCallback();

    public abstract void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback);

    public abstract StreamManager StreamManager();

    public void receive(TransportClient client, ByteBuffer message) {
        receive(client, message, ONE_WAY_CALLBACK);
    }

    public void channelActive(TransportClient client) {}

    public void channelInactive(TransportClient client) {}

    public void exceptionCaught(TransportClient client, Throwable e) {}

    @Slf4j
    private static class OneWayCallback implements RpcResponseCallback {

        @Override
        public void onSuccess(ByteBuffer message) {
            log.warn("Response provided ONE-WAY RPC.");
        }

        @Override
        public void onFailure(Throwable e) {
            log.warn("Response provided ONE-WAY RPC.");
        }
    }
}
