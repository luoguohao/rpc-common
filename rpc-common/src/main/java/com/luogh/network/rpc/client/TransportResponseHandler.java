package com.luogh.network.rpc.client;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.luogh.network.rpc.common.*;
import com.luogh.network.rpc.protocol.*;
import com.luogh.network.rpc.util.NettyUtil;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import static com.luogh.network.rpc.util.NettyUtil.getRemoteHost;

/**
 * @author luogh
 */
@Slf4j
public class TransportResponseHandler implements TransportMessageHandler<ResponseMessage> {

    private final SocketChannel channel;
    private final Map<Long, RpcResponseCallback> rpcCallbackMap;
    private final Map<StreamChunkId, ChunkReceivedCallback> streamChunkCallBackMap;
    private final Queue<StreamReceivedCallback> streamCallbackQueue;
    private final AtomicLong lastUpdateTimeInNs;
    private volatile boolean streamActive;

    public TransportResponseHandler(SocketChannel channel) {
        this.channel = channel;
        this.rpcCallbackMap = Maps.newConcurrentMap();
        this.streamChunkCallBackMap = Maps.newConcurrentMap();
        this.streamCallbackQueue = Queues.newConcurrentLinkedQueue();
        lastUpdateTimeInNs = new AtomicLong(0);
    }


    @Override
    public void handle(ResponseMessage message) throws Exception {
        if (message instanceof RpcResponseMessage) {
            RpcResponseMessage mes = (RpcResponseMessage)message;
            RpcResponseCallback callback = rpcCallbackMap.get(mes.getRequestId());
            if (callback == null) {
                log.warn("Ignore response for RPC {} from {} ({} bytes) since it is not outstanding.",
                        mes.getRequestId(), getRemoteHost(channel), mes.body().size());
            } else {
                rpcCallbackMap.remove(mes.getRequestId());
                try {
                    callback.onSuccess(mes.body().nioByteBuffer());
                } finally {
                    mes.body().release();
                }
            }
        } else if (message instanceof RpcFailureMessage) {
            RpcFailureMessage mes = (RpcFailureMessage)message;
            RpcResponseCallback callback = rpcCallbackMap.get(mes.getRequestId());
            if (callback == null) {
                log.warn("Ignore response for RPC {} from {} ({} bytes) since it is not outstanding.",
                        mes.getRequestId(), getRemoteHost(channel), mes.body().size());
            } else {
                rpcCallbackMap.remove(mes.getRequestId());
                callback.onFailure(new RuntimeException(mes.getError()));
            }
        } else if (message instanceof ChunkFetchSuccess) {
            ChunkFetchSuccess mes = (ChunkFetchSuccess) message;
            StreamChunkId streamChunkId = new StreamChunkId(mes.getStreamId(), mes.getChunkIndex());
            ChunkReceivedCallback callback = streamChunkCallBackMap.get(streamChunkId);
            if (callback == null) {
                log.warn("Ignore response for block {} from {} since it is not outstanding.",
                        streamChunkId, NettyUtil.getRemoteHost(channel));
                mes.body().release();
            } else {
                streamChunkCallBackMap.remove(streamChunkId);
                callback.onSuccess(mes.getChunkIndex(), mes.body());
                mes.body().release();
            }
        } else if (message instanceof ChunkFetchFailure) {
            ChunkFetchFailure mes = (ChunkFetchFailure) message;
            StreamChunkId streamChunkId = new StreamChunkId(mes.getStreamId(), mes.getChunkIndex());
            ChunkReceivedCallback callback = streamChunkCallBackMap.get(streamChunkId);
            if (callback == null) {
                log.warn("Ignore response for block {} from {} since it is not outstanding.",
                        streamChunkId, NettyUtil.getRemoteHost(channel));
                mes.body().release();
            } else {
                streamChunkCallBackMap.remove(streamChunkId);
                callback.onFailure(mes.getChunkIndex(), new ChunkFetchFailureException(
                        String.format("Failure while fetching %s : %s.", streamChunkId, mes.getError())));
            }

        } else if (message instanceof StreamResponse) {
            StreamResponse msg = (StreamResponse) message;
            StreamReceivedCallback callback = streamCallbackQueue.poll();
            if (callback != null) {
                if (msg.getByteCount() > 0) {
                    StreamInterceptor interceptor = new StreamInterceptor(this,
                            msg.getStreamId(), msg.getByteCount(), callback);
                    try {
                        TransportFrameDecoder handler = (TransportFrameDecoder)channel.pipeline()
                                .get(TransportFrameDecoder.HANDLER_NAME);
                        handler.setInterceptor(interceptor);
                        streamActive = true;
                    } catch (Exception e) {
                        log.error("Error installing stream handler.", e);
                        deactivateStream();
                    }
                } else {
                    try {
                        callback.onComplete(msg.getStreamId());
                    } catch (Exception e) {
                        log.warn("Error in stream handler onCompleted.", e);
                    }
                }
            } else {
                log.error("Could not find callback for StreamResponse.");
            }
        } else if (message instanceof StreamFailure) {
            StreamFailure res = (StreamFailure) message;
            StreamReceivedCallback callback = streamCallbackQueue.poll();
            if (callback != null) {
                try {
                    callback.onFailure(res.getStreamId(), new RuntimeException(res.getError()));
                } catch (Exception e) {
                    log.warn("Error in stream failure handler.", e);
                }
            } else {
                log.warn("Stream failure with unknown callback: {}.", res.getError());
            }
        } else {
            throw new IllegalStateException("Unknown response type." + message.type());
        }
    }

    public void deactivateStream() {
        this.streamActive = false;
    }

    @Override
    public void channelActive() {

    }

    @Override
    public void exceptionCaught(Throwable e) {
        if (numberOfOutstandingRequest() > 0) {
            String remoteHost = getRemoteHost(channel);
            log.error("Still have {} requests outstanding when connection from {} is closed.",
                    numberOfOutstandingRequest(), remoteHost);
            failOutstandingRequests(e);
        }
    }

    @Override
    public void channelInactive() {
        if (numberOfOutstandingRequest() > 0) {
            String remoteHost = getRemoteHost(channel);
            log.error("Still have {} requests outstanding when connection from {} is closed.",
                    numberOfOutstandingRequest(), remoteHost);
            failOutstandingRequests(new IOException("Connection from " + remoteHost + " closed."));
        }
    }

    private void failOutstandingRequests(Throwable e) {
        rpcCallbackMap.forEach((k ,v) -> v.onFailure(e));
        streamChunkCallBackMap.forEach((k, v) -> v.onFailure(k.getChunkIndex(), e));
        rpcCallbackMap.clear();
        streamChunkCallBackMap.clear();
    }

    public long lastRequestUpdateInNs() {
        return lastUpdateTimeInNs.get();
    }

    public void updateLastRequestTimeInNs() {
        lastUpdateTimeInNs.set(System.nanoTime());
    }

    public int numberOfOutstandingRequest() {
        return rpcCallbackMap.size() + streamChunkCallBackMap.size() + (streamActive ? 1 : 0);
    }

    public void addRpcRequest(long requestId, RpcResponseCallback callback) {
        updateLastRequestTimeInNs();
        rpcCallbackMap.putIfAbsent(requestId, callback);
    }

    public void removeRpcRequest(long requestId) {
        rpcCallbackMap.remove(requestId);
    }

    public void addStreamCallback(StreamReceivedCallback callback) {
        updateLastRequestTimeInNs();
        streamCallbackQueue.offer(callback);
    }

    public void addFetchChunkRequest(StreamChunkId streamChunkId, ChunkReceivedCallback callBack) {
        updateLastRequestTimeInNs();
        streamChunkCallBackMap.putIfAbsent(streamChunkId, callBack);
    }

    public void removeFetchChunkRequest(StreamChunkId streamChunkId) {
        streamChunkCallBackMap.remove(streamChunkId);
    }
}
