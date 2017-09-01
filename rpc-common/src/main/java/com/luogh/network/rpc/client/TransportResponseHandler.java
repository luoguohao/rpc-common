package com.luogh.network.rpc.client;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.luogh.network.rpc.common.StreamChunkId;
import com.luogh.network.rpc.common.TransportMessageHandler;
import com.luogh.network.rpc.protocol.ResponseMessage;
import com.luogh.network.rpc.protocol.RpcFailureMessage;
import com.luogh.network.rpc.protocol.RpcResponseMessage;
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
    private final Map<StreamChunkId, ChunkReceivedCallBack> streamChunkCallBackMap;
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
        }
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

    public void addFetchChunkRequest(StreamChunkId streamChunkId, ChunkReceivedCallBack callBack) {
        updateLastRequestTimeInNs();
        streamChunkCallBackMap.putIfAbsent(streamChunkId, callBack);
    }

    public void removeFetchChunkRequest(StreamChunkId streamChunkId) {
        streamChunkCallBackMap.remove(streamChunkId);
    }
}
