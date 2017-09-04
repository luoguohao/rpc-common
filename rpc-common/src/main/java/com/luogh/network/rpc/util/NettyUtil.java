package com.luogh.network.rpc.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.luogh.network.rpc.common.IOMode;
import com.luogh.network.rpc.common.TransportFrameDecoder;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.internal.PlatformDependent;

import java.lang.reflect.Field;
import java.util.concurrent.ThreadFactory;

/**
 * @author luogh
 */
public class NettyUtil {
    public static String getRemoteHost(Channel channel) {
        if (channel != null && channel.remoteAddress() != null) {
            return channel.remoteAddress().toString();
        }
        return "<unknown remote>";
    }

    public static EventLoopGroup createEventLoop(IOMode ioMode, int threadNum, String threadNamePrefix) {
        ThreadFactory factory = createThreadFactory(threadNamePrefix);
        switch (ioMode) {
            case NIO:
                return new NioEventLoopGroup(threadNum, factory);
            case EPOLL:
                return new EpollEventLoopGroup(threadNum, factory);
                default:
                    throw new IllegalArgumentException("Invalid IOMode:" + ioMode);
        }
    }

    public static ThreadFactory createThreadFactory(String threadNamePrefix) {
        return new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat(threadNamePrefix + "-%d")
                .build();
    }

    public static PooledByteBufAllocator createPooledByteBufAllocator(boolean preferDirectBufs, boolean allowThreadCache, int numCores) {
        if (numCores == 0) {
            numCores = Runtime.getRuntime().availableProcessors();
        }
        return new PooledByteBufAllocator(preferDirectBufs && PlatformDependent.directBufferPreferred(),
                Math.min(getPrivateStaticField("DEFAULT_NUM_HEAP_ARENA"), numCores),
                Math.min(getPrivateStaticField("DEFAULT_NUM_DIRECT_ARENA"), preferDirectBufs ? numCores : 0),
                getPrivateStaticField("DEFAULT_PAGE_SIZE"),
                getPrivateStaticField("DEFAULT_MAX_ORDER"),
                allowThreadCache ? getPrivateStaticField("DEFAULT_TINY_CACHE_SIZE") : 0,
                allowThreadCache ? getPrivateStaticField("DEFAULT_SMALL_CACHE_SIZE") : 0,
                allowThreadCache ? getPrivateStaticField("DEFAULT_NORMAL_CACHE_SIZE") : 0
                );
    }

    private static int getPrivateStaticField(String staticFiled) {
        try {
            Field field = PooledByteBufAllocator.DEFAULT.getClass().getDeclaredField(staticFiled);
            field.setAccessible(true);
            return field.getInt(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static Class<? extends ServerChannel> getServerChannelClass(IOMode ioMode) {
        switch (ioMode) {
            case EPOLL:
                return EpollServerSocketChannel.class;
            case NIO:
                return NioServerSocketChannel.class;
                default:
                    throw new IllegalArgumentException("Invalid ioMode=" + ioMode);
        }
    }

    public static Class<? extends SocketChannel> getClientChannelClass(IOMode ioMode) {
        switch (ioMode) {
            case EPOLL:
                return EpollSocketChannel.class;
            case NIO:
                return NioSocketChannel.class;
            default:
                throw new IllegalArgumentException("Invalid ioMode=" + ioMode);
        }
    }

    public static TransportFrameDecoder createFrameDecoder() {
        return new TransportFrameDecoder();
    }
}
