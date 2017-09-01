package com.luogh.network.rpc.server;

import com.luogh.network.rpc.common.RpcHandler;
import io.netty.channel.Channel;

/**
 * @author luogh
 */
public interface TransportServerBootstrap {
    RpcHandler doBootstrap(Channel channel, RpcHandler rpcHandler);
}
