package com.luogh.network.rpc.client;

import io.netty.channel.Channel;

/**
 * @author luogh
 */
public interface TransportClientBootstrap {
    void doBootStrap(TransportClient client, Channel channel) throws Exception;
}
