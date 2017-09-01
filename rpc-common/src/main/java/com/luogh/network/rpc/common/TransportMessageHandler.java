package com.luogh.network.rpc.common;

import com.luogh.network.rpc.protocol.Message;

/**
 * @author luogh
 */
public interface TransportMessageHandler<T extends Message> {

    void handle(T message) throws Exception;

    void channelActive();

    void exceptionCaught(Throwable e);

    void channelInactive();

}
