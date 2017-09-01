package com.luogh.network.rpc.server;

import com.google.common.base.Throwables;
import com.luogh.network.rpc.buffer.NioManagedBuffer;
import com.luogh.network.rpc.client.RpcResponseCallback;
import com.luogh.network.rpc.client.TransportClient;
import com.luogh.network.rpc.common.RpcHandler;
import com.luogh.network.rpc.common.TransportMessageHandler;
import com.luogh.network.rpc.protocol.*;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.socket.SocketChannel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.net.SocketAddress;
import java.nio.ByteBuffer;

/**
 * @author luogh
 */
@Slf4j
@Getter
public class TransportRequestHandler implements TransportMessageHandler<RequestMessage> {

    private final SocketChannel channel;
    private final TransportClient client;
    private final RpcHandler rpcHandler;

    public TransportRequestHandler(SocketChannel channel, TransportClient client, RpcHandler rpcHandler) {
        this.channel = channel;
        this.client = client;
        this.rpcHandler = rpcHandler;
    }

    @Override
    public void handle(RequestMessage message) throws Exception {
        if (message instanceof RpcRequestMessage) {
            processRpcRequest((RpcRequestMessage)message);
        }
    }

    private void processRpcRequest(RpcRequestMessage message) {
        try {
            rpcHandler.receive(client, message.body().nioByteBuffer(), new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    response(new RpcResponseMessage(new NioManagedBuffer(response), message.getRequestId()));
                }

                @Override
                public void onFailure(Throwable e) {
                    response(new RpcFailureMessage(message.getRequestId(), Throwables.getStackTraceAsString(e)));
                }
            });
        } catch (Exception e) {
            log.error("Error while invoking RpcHandler#processRpcRequest On RPC id " + message.getRequestId(), e);
            response(new RpcFailureMessage(message.getRequestId(), Throwables.getStackTraceAsString(e)));
        } finally {
            message.body().release();
        }

    }

    private void response(Encodeable message) {
        final SocketAddress client = channel.remoteAddress();
        channel.writeAndFlush(message).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                log.trace("Sent result:{} to client {}.", message, client);
            } else {
                log.error(String.format("Send result %s to client %s failed, close connection.",
                        message, client), future.cause());
                channel.close();
            }
        });
    }

    @Override
    public void channelActive() {
        rpcHandler.channelActive(client);
    }

    @Override
    public void exceptionCaught(Throwable e) {
        rpcHandler.exceptionCaught(client, e);
    }

    @Override
    public void channelInactive() {
        rpcHandler.channelInactive(client);
    }
}
