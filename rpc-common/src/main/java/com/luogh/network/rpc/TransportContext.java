package com.luogh.network.rpc;

import com.google.common.collect.Lists;
import com.luogh.network.rpc.client.TransportClient;
import com.luogh.network.rpc.client.TransportClientBootstrap;
import com.luogh.network.rpc.client.TransportResponseHandler;
import com.luogh.network.rpc.common.*;
import com.luogh.network.rpc.server.TransportRequestHandler;
import com.luogh.network.rpc.server.TransportServer;
import com.luogh.network.rpc.server.TransportServerBootstrap;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author luogh
 */
@Slf4j
public class TransportContext {

    @Getter
    private final TransportConf transportConf;
    private final RpcHandler rpcHandler;
    private final MessageDecoder messageDecoder;
    private final MessageEncoder messageEncoder;
    private final boolean closeIdleConnections;

    public TransportContext(TransportConf transportConf, RpcHandler rpcHandler) {
       this(transportConf, rpcHandler, false);
    }

    public TransportContext(TransportConf transportConf, RpcHandler rpcHandler, boolean closeIdleConnections) {
        this.transportConf = transportConf;
        this.rpcHandler = rpcHandler;
        this.messageDecoder = new MessageDecoder();
        this.messageEncoder = new MessageEncoder();
        this.closeIdleConnections = closeIdleConnections;
    }


    public TransportServer createServer(String host, int port, List<TransportServerBootstrap> bootstraps) {
        return new TransportServer(this, host, port, rpcHandler, bootstraps);
    }

    public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
        return createServer(null, port, bootstraps);
    }

    public TransportServer createServer(List<TransportServerBootstrap> bootstraps) {
        return createServer(0, Lists.newArrayList());
    }

    public TransportServer createServer(int port) {
        return createServer(port, Lists.newArrayList());
    }

    public TransportServer createServer() {
        return createServer(0, Lists.newArrayList());
    }

    public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
        return new TransportClientFactoryImpl(this, bootstraps);
    }

    public TransportClientFactory createClientFactory() {
        return createClientFactory(Lists.newArrayList());
    }

    public TransportChannelHandler initChannelPipeline(SocketChannel channel) {
        return initChannelPipeline(channel, rpcHandler);
    }

    public TransportChannelHandler initChannelPipeline(SocketChannel channel, RpcHandler rpcHandler) {
        try {
            TransportChannelHandler channelHandler = createTransportChannelHandler(channel, rpcHandler);
            channel.pipeline()
                    .addLast("encoder", messageEncoder)
                    .addLast("frameDecoder", new TransportFrameDecoder())
                    .addLast("decoder", messageDecoder)
                    .addLast("idleStateHandler", new IdleStateHandler(
                            0, 0,
                            transportConf.connectionTimeoutMs() / 1000))
                    .addLast("handler", channelHandler);
            return channelHandler;
        } catch (Exception e) {
            throw new RuntimeException("init channel pipeline failed.", e);
        }

    }

    private TransportChannelHandler createTransportChannelHandler(SocketChannel channel, RpcHandler rpcHandler) {
        TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
        TransportClient client = new TransportClient(channel, responseHandler);
        TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client, rpcHandler);
        return new TransportChannelHandler(requestHandler, responseHandler, transportConf.connectionTimeoutMs(),
                closeIdleConnections);
    }

}
