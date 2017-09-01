package com.luogh.network.rpc.client;

import com.google.common.base.MoreObjects;
import com.luogh.network.rpc.buffer.NioManagedBuffer;
import com.luogh.network.rpc.common.StreamChunkId;
import com.luogh.network.rpc.protocol.ChunkFetchRequestMessage;
import com.luogh.network.rpc.protocol.OneWayMessage;
import com.luogh.network.rpc.protocol.RpcRequestMessage;
import com.luogh.network.rpc.protocol.StreamRequest;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.socket.SocketChannel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.luogh.network.rpc.util.NettyUtil.getRemoteHost;

/**
 * @author luogh
 */
@Slf4j
public class TransportClient implements Closeable {

    @Getter
    private final SocketChannel channel;
    @Getter
    private final TransportResponseHandler responseHandler;
    private volatile boolean timeout;
    @Getter
    private String clientId;


    public TransportClient(SocketChannel channel, TransportResponseHandler responseHandler) {
        this.channel = channel;
        this.responseHandler = responseHandler;
    }

    public void stream(String streamId, final StreamReceivedCallback callback) {
        long startTime = System.currentTimeMillis();
        log.trace("Sending streaming {} request to {}.", streamId, getRemoteHost(channel));
        // need synchronized this to ensure that add to streamCallback queue and write flush
        // to channel happens atomic. so that stream callback will be called in the right order
        // when responses arrives.
        synchronized (this) {
            responseHandler.addStreamCallback(callback);
            channel.writeAndFlush(new StreamRequest(streamId)).addListener(
                    (ChannelFutureListener) future -> {
                        if (future.isSuccess()) {
                            long costTime = System.currentTimeMillis() - startTime;
                            log.trace("Send streaming {} to {} cost {} ms.", streamId,
                                    getRemoteHost(channel),
                                    costTime);
                        } else {
                            String errorMessage = String.format("Send streaming %s to %s failed, with error %s.",
                                    streamId, getRemoteHost(channel), future.cause());
                            log.error(errorMessage, future.cause());
                            channel.close();
                            try {
                                callback.onFailure(streamId, new IOException(errorMessage, future.cause()));
                            } catch (Exception e) {
                                log.error("Uncaught exception in stream request.", e);
                            }
                        }
                    }
            );
        }
    }


    public void fetchChunk(String streamId, int chunkIndex, final ChunkReceivedCallBack callBack) {
        final long startTime = System.currentTimeMillis();
        log.trace("Sending fetchChunk request streamId {} chunkIndex {} to {}.", streamId, chunkIndex,
                getRemoteHost(channel));
        StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIndex);
        responseHandler.addFetchChunkRequest(streamChunkId, callBack);
        channel.writeAndFlush(new ChunkFetchRequestMessage(streamId, chunkIndex)).addListener(
                (ChannelFutureListener) future -> {
                    if (future.isSuccess()) {
                        long costTime = System.currentTimeMillis() - startTime;
                        log.trace("Send fetchChunk request streamId {} chunkIndex {} to {} cost {} ms.",
                                streamId, chunkIndex, getRemoteHost(channel), costTime);
                    } else {
                        String errorMessage = String.format("Send fetchChunk request streamId %s chunkIndex %d " +
                                "to %s failed with error %s .", streamId, chunkIndex, future.cause());
                        log.error(errorMessage);
                        responseHandler.removeFetchChunkRequest(streamChunkId);
                        try {
                            callBack.onFailure(chunkIndex, new IOException(errorMessage, future.cause()));
                        } catch (Exception e) {
                            log.error("Uncaught exception in RPC callback response.", e);
                        }
                    }
                }
        );
    }


    public long sendRpc(ByteBuffer message, final RpcResponseCallback callback) {
        final long startTime = System.currentTimeMillis();
        log.trace("Sending RPC to {}.", getRemoteHost(channel));

        final long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
        responseHandler.addRpcRequest(requestId, callback);
        channel.writeAndFlush(new RpcRequestMessage(new NioManagedBuffer(message), requestId))
                .addListener((ChannelFutureListener) future -> {
                    if (future.isSuccess()) {
                        long timeToken = System.currentTimeMillis() - startTime;
                        log.trace("Sending request {} to {} took {} ms.", requestId,
                                getRemoteHost(channel), timeToken);

                    } else {
                        String errorMessage = String.format("Failed to send RPC %s to %s: %s",
                                requestId, getRemoteHost(channel), future.cause());
                        log.error(errorMessage, future.cause());
                        responseHandler.removeRpcRequest(requestId);
                        channel.close();
                        try {
                            callback.onFailure(new IOException(errorMessage, future.cause()));
                        } catch (Exception e) {
                            log.error("Uncaught exception in RPC response callback handler.", e);
                        }
                    }
                });
        return requestId;
    }

    public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) {
        final CompletableFuture<ByteBuffer> completableFuture = new CompletableFuture<>();
        sendRpc(message, new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer message) {
                ByteBuffer buffer = ByteBuffer.allocate(message.remaining());
                buffer.put(message);
                buffer.flip(); // flip to make it readable.
                completableFuture.complete(buffer);
            }

            @Override
            public void onFailure(Throwable e) {
                completableFuture.completeExceptionally(e);
            }
        });

        try {
            return completableFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void send(ByteBuffer message) {
        channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message)));
    }

    public void setClientId(String id) {
        if (clientId != null) {
            throw new RuntimeException("TransportClient already assigned a id=" + clientId);
        }
        this.clientId = id;
    }

    @Override
    public void close() throws IOException {
        channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
    }

    public void timeout() {
        this.timeout = true;
    }

    public boolean isAlive() {
        return !this.timeout && (channel.isActive() || channel.isOpen());
    }

    public SocketAddress geSocketAddress() {
        return this.channel.remoteAddress();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("remoteAddress", channel.remoteAddress())
                .add("clientId", clientId)
                .add("isAlive", isAlive())
                .toString();
    }
}
