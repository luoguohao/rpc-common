package com.luogh.network.rpc.server;

import com.luogh.network.rpc.TransportContext;
import com.luogh.network.rpc.common.IOMode;
import com.luogh.network.rpc.common.RpcHandler;
import com.luogh.network.rpc.common.TransportConf;
import com.luogh.network.rpc.util.NettyUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author luogh
 */
@Slf4j
public class TransportServer implements Closeable {

    private final TransportContext transportContext;
    private final RpcHandler rpcHandler;
    private final List<TransportServerBootstrap> bootstraps;
    private final TransportConf transportConf;

    private int port = -1;
    private ServerBootstrap bootstrap;
    private ChannelFuture channelFuture;

    public TransportServer(TransportContext transportContext, String host, int port,
                           RpcHandler rpcHandler, List<TransportServerBootstrap> bootstraps) {
        this.transportContext = transportContext;
        this.transportConf = transportContext.getTransportConf();
        this.rpcHandler = rpcHandler;
        this.bootstraps = bootstraps;

        try {
            init(host, port);
        } catch (Exception e) {

        }
    }

    public int getPort() {
        if (port == -1) throw new IllegalStateException("Server not started.");
        return port;
    }

    private void init(String host, int port) {
        IOMode ioMode = IOMode.valueOf(transportConf.ioMode());
        EventLoopGroup bossGroup =
                NettyUtil.createEventLoop(ioMode, transportConf.serverThread(),
                        transportConf.getModuleName() + "-server");
        EventLoopGroup workerGroup = bossGroup;

        PooledByteBufAllocator allocator = NettyUtil.createPooledByteBufAllocator(
                transportConf.preferDirectBufs(), true, transportConf.serverThread()
        );

        bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NettyUtil.getServerChannelClass(ioMode))
                .option(ChannelOption.ALLOCATOR, allocator)
                .childOption(ChannelOption.ALLOCATOR, allocator);

        if (transportConf.backLog() > 0) {
            bootstrap.option(ChannelOption.SO_BACKLOG, transportConf.backLog());
        }
        if (transportConf.receiveBuf() > 0) {
            bootstrap.childOption(ChannelOption.SO_RCVBUF, transportConf.receiveBuf());
        }
        if (transportConf.sendBuf() > 0) {
            bootstrap.childOption(ChannelOption.SO_SNDBUF, transportConf.sendBuf());
        }

        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
              RpcHandler handler = rpcHandler;
              for (TransportServerBootstrap boot : bootstraps) {
                  handler = boot.doBootstrap(ch, handler);
              }
              transportContext.initChannelPipeline(ch, handler);
            }
        });

        InetSocketAddress address = host == null ? new InetSocketAddress(port) : new InetSocketAddress(host, port);
        channelFuture = bootstrap.bind(address);
        channelFuture.syncUninterruptibly();

        port = ((InetSocketAddress)(channelFuture.channel().localAddress())).getPort();
        log.debug("server started at port:" + port);
    }

    @Override
    public void close() throws IOException {
        if (channelFuture != null) {
            channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS);
            channelFuture = null;
        }
        if (bootstrap != null && bootstrap.config().group() != null) {
            bootstrap.config().group().shutdownGracefully();
        }
        if (bootstrap != null && bootstrap.config().childGroup() != null) {
            bootstrap.config().childGroup().shutdownGracefully();
        }
        bootstrap = null;
    }
}
