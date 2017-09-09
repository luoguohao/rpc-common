package com.luogh.network.rpc;

import com.google.common.collect.ImmutableMap;
import com.luogh.network.rpc.buffer.ManagedBuffer;
import com.luogh.network.rpc.client.ChunkReceivedCallback;
import com.luogh.network.rpc.client.RpcResponseCallback;
import com.luogh.network.rpc.client.TransportClient;
import com.luogh.network.rpc.common.*;
import com.luogh.network.rpc.server.TransportServer;
import com.luogh.network.rpc.util.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author luogh
 */
public class RequestTimeoutIntegrationSuite {

    private TransportConf conf;
    private TransportClientFactory factory;
    private TransportServer server;

    @Before
    public void init() {
        Map<String, String> maps = ImmutableMap.of("test.io.connectionTimeout", "10s");
        conf = new TransportConf("test", new MapConfigProvider(maps));
    }

    @Test
    public void timeoutInactiveRequests() throws Exception {
        final Semaphore signal = new Semaphore(1);
        final int responseSize = 16;
        RpcHandler rpcHandler = new RpcHandler() {
            @Override
            public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
                try {
                    signal.acquire();
                    callback.onSuccess(ByteBuffer.allocate(responseSize));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public StreamManager streamManager() {
                return null;
            }
        };

        final TransportContext context = new TransportContext(conf, rpcHandler);
        server = context.createServer();
        factory = context.createClientFactory();
        TransportClient client = factory.createClient(Util.getLocalHost(), server.getPort());

        TestCallback testCallback = new TestCallback();
        // First completes quickly
        client.sendRpc(ByteBuffer.allocate(0), testCallback);
        testCallback.latch.await();
        assertEquals(responseSize, testCallback.successLength);

        // Second times out after 10s, with slack. Must be IOException.
        TestCallback testCallback1 = new TestCallback();
        client.sendRpc(ByteBuffer.allocate(0), testCallback1);
        testCallback1.latch.await(60, TimeUnit.SECONDS); // this will timeout cause rpcHandler semaphore can't get a available signal.
        assertNotNull(testCallback1.failure);
        assertTrue(testCallback1.failure instanceof IOException);

        signal.release();
    }


    /**
     * A timeout will cause connection to be closed, invalidating the current TransportClient.
     * It should be the case that requesting a client from a factory produces a new, valid one.
     * @throws Exception
     */
    @Test
    public void timeoutCleanlyCloseClient() throws Exception {
        final Semaphore signal = new Semaphore(0);
        final int responseSize = 16;

        RpcHandler rpcHandler = new RpcHandler() {
            @Override
            public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
                try {
                    signal.acquire();
                    callback.onSuccess(ByteBuffer.allocate(responseSize));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public StreamManager streamManager() {
                return null;
            }
        };

        final TransportContext context = new TransportContext(conf, rpcHandler);
        server = context.createServer();
        factory = context.createClientFactory();

        // First request should failed eventually fail.
        final TransportClient client0  = factory.createClient(Util.getLocalHost(), server.getPort());
        final TestCallback callback0 = new TestCallback();
        client0.sendRpc(ByteBuffer.allocate(0), callback0);
        callback0.latch.await();
        assertTrue(callback0.failure instanceof IOException);
        assertTrue(!client0.isAlive());

        // Increment the semaphore and the second request should succeed quickly.
        signal.release(2);
        TransportClient client1 = factory.createClient(Util.getLocalHost(), server.getPort());
        final TestCallback callback1 = new TestCallback();
        client1.sendRpc(ByteBuffer.allocate(0), callback1);
        callback1.latch.await();
        assertEquals(responseSize, callback1.successLength);
        assertNull(callback1.failure);
    }

    @After
    public void shutdown() {
        if (server != null) {
            try {
                server.close();
            } catch (IOException e) {
                throw new RuntimeException("shutdown transport server failed.", e);
            }
        }

        if (factory != null) {
            try {
                factory.close();
            } catch (IOException e) {
                throw new RuntimeException("shutdown transport client factory failed.", e);
            }
        }
    }

    static class TestCallback implements RpcResponseCallback, ChunkReceivedCallback {

        int successLength = -1;
        Throwable failure;
        final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void onSuccess(ByteBuffer message) {
            successLength = message.remaining();
            latch.countDown();
        }

        @Override
        public void onFailure(Throwable e) {
            failure = e;
            latch.countDown();
        }

        @Override
        public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
            try {
                successLength = buffer.nioByteBuffer().remaining();
            } catch (Exception e) {

            } finally {
                latch.countDown();
            }
        }

        @Override
        public void onFailure(int chunkIndex, Throwable e) {
            failure = e;
            latch.countDown();
        }
    }
}
