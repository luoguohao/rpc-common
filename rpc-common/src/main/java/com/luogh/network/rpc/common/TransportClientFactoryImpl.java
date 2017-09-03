package com.luogh.network.rpc.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.luogh.network.rpc.TransportContext;
import com.luogh.network.rpc.client.TransportClient;
import com.luogh.network.rpc.client.TransportClientBootstrap;
import com.luogh.network.rpc.util.NettyUtil;
import com.luogh.network.rpc.util.Util;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author luogh
 */
@Slf4j
public class TransportClientFactoryImpl implements TransportClientFactory {

    private final TransportContext context;
    private final List<TransportClientBootstrap> bootstraps;
    private final TransportConf conf;
    private final ConcurrentMap<SocketAddress, ClientPool> connectionPool;

    private final Random rand;
    private final int numConnectionPerPeer;
    private final Class<? extends SocketChannel> socketChannelClass;
    private final EventLoopGroup workerGroup;
    private final PooledByteBufAllocator allocator;

    private static class ClientPool {
        TransportClient[] clients;
        Object[] locks;

        ClientPool(int size) {
            clients = new TransportClient[size];
            locks = new Object[size];
            for (int i = 0; i < size; i++) {
                locks[i] = new Object();
            }
        }
    }

    public TransportClientFactoryImpl(TransportContext transportContext, List<TransportClientBootstrap> bootstraps) {
        this.context = Preconditions.checkNotNull(transportContext);
        this.conf = transportContext.getTransportConf();
        this.bootstraps = bootstraps;
        this.connectionPool = Maps.newConcurrentMap();
        this.numConnectionPerPeer = conf.numConnectionsPerPeer();
        this.rand = new Random();

        IOMode ioMode = IOMode.valueOf(conf.ioMode());
        this.socketChannelClass = NettyUtil.getClientChannelClass(ioMode);
        this.workerGroup = NettyUtil.createEventLoop(ioMode,
                conf.clientThreads(),
                conf.getModuleName() + "-client");
        this.allocator = NettyUtil.createPooledByteBufAllocator(conf.preferDirectBuf(), false,
                conf.clientThreads());
    }

    @Override
    public TransportClient createClient(String remoteHost, int remotePort) throws InterruptedException, IOException {
        Preconditions.checkArgument(StringUtils.isNotBlank(remoteHost) && remotePort > 0 ,
                "Invalid remoteHost=" + remoteHost + ", remotePort=" + remotePort);

        final InetSocketAddress remoteAddr = InetSocketAddress.createUnresolved(remoteHost, remotePort);

        ClientPool pool = connectionPool.get(remoteAddr);
        if (pool == null) {
            connectionPool.putIfAbsent(remoteAddr, new ClientPool(numConnectionPerPeer));
            pool = connectionPool.get(remoteAddr);
        }

        int randNext = rand.nextInt(numConnectionPerPeer);
        TransportClient cachedClient = pool.clients[randNext];
        if (cachedClient != null && cachedClient.isAlive()) {
            TransportChannelHandler handler = cachedClient.getChannel().pipeline().get(TransportChannelHandler.class);
            synchronized (handler) { // synchronized TransportChannelHandler to avoid IdleState checking using lastRequestTime.
                handler.getResponseHandler().updateLastRequestTimeInNs();
            }
            if (cachedClient.isAlive()) {
                log.trace("Returning  cached connection to {}:{}", cachedClient.geSocketAddress(), cachedClient);
                return cachedClient;
            }
        }


        // if we reached here ,that means we have not a active connection to reuse, so ,create a new one.
        final long preTime = System.nanoTime();
        final InetSocketAddress resovledAddr = new InetSocketAddress(remoteHost, remotePort);
        final long hostResolvedCostTime = (System.nanoTime() - preTime) / 1000000;
        if (hostResolvedCostTime > 2000) {
            log.warn("DNS resolution for {} took {} ms.", resovledAddr, hostResolvedCostTime);
        } else {
            log.trace("DNS resolution for {} took {} ms.", resovledAddr, hostResolvedCostTime);
        }

        synchronized (pool.locks[randNext]) {
            cachedClient = pool.clients[randNext];
            if (cachedClient != null) {
                if (cachedClient.isAlive()) {
                    log.trace("Returning cached connection to {}:{}.", cachedClient.geSocketAddress(), cachedClient);
                    return cachedClient;
                } else {
                    log.info("Found inactive connection to {}, creating a new one.", cachedClient.geSocketAddress());
                }
            }
            pool.clients[randNext] = createClient(resovledAddr);
            return pool.clients[randNext];
        }
    }

    private TransportClient createClient(InetSocketAddress resovledAddr) throws InterruptedException, IOException {
        log.debug("Create new connection to {}.", resovledAddr);
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.channel(this.socketChannelClass)
                .group(workerGroup)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs())
                .option(ChannelOption.ALLOCATOR, this.allocator);

        final AtomicReference<TransportClient> clientRef = new AtomicReference<>();
        final AtomicReference<SocketChannel> channelRef = new AtomicReference<>();

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                TransportChannelHandler handler = context.initChannelPipeline(ch);
                clientRef.set(handler.getClient());
                channelRef.set(ch);

            }
        });

        ChannelFuture future = bootstrap.connect(resovledAddr);
        if (!future.await(conf.connectionTimeoutMs())) {
            throw new IOException(
              String.format("Connecting to %s time out (%s ms)", resovledAddr, conf.connectionTimeoutMs())
            );
        } else if (future.cause() != null) {
            throw new IOException(String.format("Failed to connect to %s", resovledAddr), future.cause());
        }

        TransportClient client = clientRef.get();
        Channel channel = channelRef.get();

        assert client != null : "Channel future completed successfully with null client.";

        long preBootstrap = System.nanoTime();
        try {
            for (TransportClientBootstrap boot : bootstraps) {
                boot.doBootStrap(client, channel);
            }
        } catch (Exception e) {
            long bootstrapTime = (System.nanoTime() - preBootstrap) / 1000_1000;
            log.error("Exception while bootstrapping the client after " + bootstrapTime + " ms");
            client.close();
            throw new RuntimeException(e);
        }

        long postBootstrap = System.nanoTime();
        log.info("Successfully created connection to {} after {} ms spent in bootstrap.",
                resovledAddr, (postBootstrap - preBootstrap) / 1000_1000);

        return client;
    }

    @Override
    public void close() throws IOException {
        for (ClientPool pool : connectionPool.values()) {
            for (int i = 0; i < pool.clients.length ; i++) {
                TransportClient client = pool.clients[i];
                if (client != null) {
                    Util.closeQuietly(client);
                    pool.clients[i] = null;
                }
            }
        }
        connectionPool.clear();
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }
}
