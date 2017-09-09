package com.luogh.network.rpc.common;

import com.luogh.network.rpc.client.TransportClient;
import com.luogh.network.rpc.client.TransportResponseHandler;
import com.luogh.network.rpc.protocol.Message;
import com.luogh.network.rpc.protocol.RequestMessage;
import com.luogh.network.rpc.protocol.ResponseMessage;
import com.luogh.network.rpc.server.TransportRequestHandler;
import com.luogh.network.rpc.util.NettyUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author luogh
 */
@Slf4j
public class TransportChannelHandler extends SimpleChannelInboundHandler<Message> {

    @Getter private final TransportClient client;
    @Getter private final TransportRequestHandler requestHandler;
    @Getter private final TransportResponseHandler responseHandler;
    private final long connectionTimeoutInNs;
    private final boolean closeIdleConnections;


    public TransportChannelHandler(TransportRequestHandler requestHandler, TransportResponseHandler responseHandler,
                                   int connectionTimeoutInMs, boolean closeIdleConnection) {
        this.client = requestHandler.getClient();
        this.requestHandler = requestHandler;
        this.responseHandler = responseHandler;
        this.connectionTimeoutInNs = connectionTimeoutInMs * 1000L * 1000L;
        this.closeIdleConnections = closeIdleConnection;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        if (msg instanceof RequestMessage) {
            requestHandler.handle((RequestMessage)msg);
        } else if (msg instanceof ResponseMessage) {
            responseHandler.handle((ResponseMessage)msg);
        } else {
            log.warn("Unknown message type.", msg);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        try {
            responseHandler.channelActive();
        } catch (Exception e) {
            log.error("Exception from responseHandler while channel is active,", e);
            throw e;
        }
        try {
            requestHandler.channelActive();
        } catch (Exception e) {
            log.error("Exception from requestHandler while channel is active,", e);
            throw e;
        }
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        try {
            responseHandler.channelInactive();
        } catch (Exception e) {
            log.error("Exception from responseHandler while channel is inactive,", e);
            throw e;
         }

        try {
            requestHandler.channelInactive();
        } catch (Exception e) {
            log.error("Exception from requestHandler while channel is inactive,", e);
            throw e;
        }

        super.channelInactive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent)evt;
            // using synchronized lock here , as TransportClientFactory.createClient() should check whether the client
            // is still alive, but in this code, we will change the Transport client active state ,so synchronized lock
            // is needed.
            synchronized (this) {
                boolean lastInterval = (System.nanoTime() - responseHandler.lastRequestUpdateInNs()) > connectionTimeoutInNs;
                if (event.state() == IdleState.ALL_IDLE && lastInterval) {
                    if (responseHandler.numberOfOutstandingRequest() > 0) {
                        log.warn("There are still some request waiting for the remote host {} response, " +
                                        "but the connection is timeout in {} ms.", NettyUtil.getRemoteHost(ctx.channel()),
                                connectionTimeoutInNs / 1000 / 1000);
                        client.timeout();
                        ctx.close();
                    } else if (closeIdleConnections) {
                        client.timeout();
                        ctx.close();
                    }
                }
            }

        }
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        try {
            responseHandler.exceptionCaught(cause);
        } catch (Exception e) {
            log.error("Exception from responseHandler while channel with exception caught,", e);
        }

        try {
            requestHandler.exceptionCaught(cause);
        } catch (Exception e) {
            log.error("Exception from requestHandler while channel with exception caught,", e);
        }
        ctx.close();
        super.exceptionCaught(ctx, cause);
    }
}
