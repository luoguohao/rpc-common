package com.luogh.network.rpc.server;

import com.google.common.base.Throwables;
import com.luogh.network.rpc.buffer.ManagedBuffer;
import com.luogh.network.rpc.buffer.NioManagedBuffer;
import com.luogh.network.rpc.client.RpcResponseCallback;
import com.luogh.network.rpc.client.TransportClient;
import com.luogh.network.rpc.common.RpcHandler;
import com.luogh.network.rpc.common.StreamManager;
import com.luogh.network.rpc.common.TransportMessageHandler;
import com.luogh.network.rpc.protocol.*;
import com.luogh.network.rpc.util.NettyUtil;
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
    private final StreamManager streamManager;

    public TransportRequestHandler(SocketChannel channel, TransportClient client, RpcHandler rpcHandler) {
        this.channel = channel;
        this.client = client;
        this.rpcHandler = rpcHandler;
        this.streamManager = rpcHandler.streamManager();
    }

    @Override
    public void handle(RequestMessage message) throws Exception {
        if (message instanceof RpcRequestMessage) {
            processRpcRequest((RpcRequestMessage)message);
        } else if (message instanceof OneWayMessage) {
            processOneWayMessage((OneWayMessage)message);
        } else if (message instanceof ChunkFetchRequestMessage) {
            processChunkFetchMessage((ChunkFetchRequestMessage)message);
        } else if (message instanceof StreamRequest) {
            processStreamMessage((StreamRequest)message);
        }
    }

    private void processStreamMessage(StreamRequest message) {
        if (log.isTraceEnabled()) {
            log.trace("Received req from {} to fetch stream {}.",
                    NettyUtil.getRemoteHost(channel), message.getStreamId());
        }
        ManagedBuffer buf;
        try {
            buf = streamManager.openStream(message.getStreamId());
        } catch (Exception e) {
            log.error(String.format("Error opening stream %s for the request from %s",
                    message.getStreamId()), NettyUtil.getRemoteHost(channel), e);
            response(new StreamFailure(message.getStreamId(), Throwables.getStackTraceAsString(e)));
            return;
        }
        if (buf != null) {
            response(new StreamResponse(message.getStreamId(), buf.size(), buf));
        } else {
            response(new StreamFailure(message.getStreamId(),
                    String.format("Stream %s not found.", message.getStreamId())));
        }

    }

    private void processChunkFetchMessage(ChunkFetchRequestMessage message) {
        if (log.isTraceEnabled()) {
            log.trace("Received req from {} to fetch block {} with index {}",
                    NettyUtil.getRemoteHost(channel), message.getStreamId(), message.getChunkIndex());
        }
        ManagedBuffer buf;
        try {
            streamManager.checkAuthorization(client, message.getStreamId());
            streamManager.registerChannel(channel, message.getStreamId());
            buf = streamManager.getChunk(message.getStreamId(), message.getChunkIndex());
        } catch (Exception e) {
            log.error(String.format("Error opening block %s with index %s for the request from %s",
                    message.getStreamId(), message.getChunkIndex()), NettyUtil.getRemoteHost(channel), e);
            response(new ChunkFetchFailure(message.getStreamId(), message.getChunkIndex(),
                    Throwables.getStackTraceAsString(e)));
            return;
        }
        response(new ChunkFetchSuccess(message.getStreamId(), message.getChunkIndex(), buf));
    }

    private void processOneWayMessage(OneWayMessage message) {
        try {
            rpcHandler.receive(client, message.body().nioByteBuffer());
        } catch (Exception e) {
            log.error("Error while invoking RpcHandler#processOneWayMessage On RPC", e);
        } finally {
            message.body().release();
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
