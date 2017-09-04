package com.luogh.network.rpc;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.luogh.network.rpc.client.RpcResponseCallback;
import com.luogh.network.rpc.client.TransportClient;
import com.luogh.network.rpc.common.*;
import com.luogh.network.rpc.server.TransportServer;
import com.luogh.network.rpc.util.Util;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author luogh
 */
public class RpcIntegrationSuite {

    private static TransportConf conf;
    private static TransportContext context;
    private static TransportClientFactory clientFactory;
    private static TransportServer server;
    private static final List<String> oneWayMsgs = Lists.newArrayList();

    @BeforeClass
    public static void setUp() throws Exception {
        conf = new TransportConf("test", new SystemPropertyConfigProvider());
        RpcHandler rpcHandler = new RpcHandler() {

            @Override
            public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
                String msg = Util.bytesToString(message);
                String[] fragments = msg.split("/");
                if (fragments[0].equals("hello")) {
                    callback.onSuccess(Util.stringToBytes("Hello," + fragments[1] + "!"));
                } else if (fragments[0].equals("return error")) {
                    callback.onFailure(new RuntimeException("Returned:" + fragments[1]));
                } else if (fragments[0].equals("throw error")){
                    throw new RuntimeException("Thrown: " + fragments[1]);
                }
            }

            @Override
            public StreamManager StreamManager() {
                return null;
            }

            @Override
            public void receive(TransportClient client, ByteBuffer message) {
                oneWayMsgs.add(Util.bytesToString(message));
            }
        };

        context = new TransportContext(conf, rpcHandler);
        server = context.createServer();
        clientFactory = context.createClientFactory();
    }

    @AfterClass
    public static void destroy() {
        try {
            if (server != null) server.close();
            if (clientFactory != null) clientFactory.close();
        } catch (Exception e) {
            throw new RuntimeException("shutdown service failed.", e);
        }
    }

    static class RpcResult {
        public Set<String> sucMsgs;
        public Set<String> failedMsgs;
    }

    private RpcResult sendRpc(String ... commands) throws Exception {
        RpcResult result = new RpcResult();
        result.sucMsgs = Sets.newConcurrentHashSet();
        result.failedMsgs = Sets.newConcurrentHashSet();
        final Semaphore signal = new Semaphore(0);

        RpcResponseCallback callback = new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer message) {
                result.sucMsgs.add(Util.bytesToString(message));
                signal.release();
            }

            @Override
            public void onFailure(Throwable e) {
                result.failedMsgs.add(e.getMessage());
                signal.release();
            }
        };

        TransportClient client = clientFactory.createClient(Util.getLocalHost(), server.getPort());

        for (String command : commands) {
            client.sendRpc(Util.stringToBytes(command), callback);
        }

        if (!signal.tryAcquire(commands.length, 5, TimeUnit.SECONDS)) {
            fail("Timeout getting response from the server.");
        }

        client.close();
        return result;
    }

    @Test
    public void singleRPC() throws Exception {
        RpcResult result = sendRpc("hello/luogh");
        assertEquals(Sets.newHashSet("Hello,luogh!"), result.sucMsgs);
        assertTrue(result.failedMsgs.isEmpty());
    }

    @Test
    public void multiRPC() throws Exception {
        RpcResult result = sendRpc("hello/luogh", "hello/test",
                "return error/connection failed", "throw error/failed compute");

        assertEquals(Sets.newHashSet("Hello,luogh!", "Hello,test!"), result.sucMsgs);
        assertErrorsContain(Sets.newHashSet("Returned:connection failed", "Thrown: failed compute"), result.failedMsgs);
    }

    @Test
    public void sendOneWayMessage() {
        try( TransportClient client = clientFactory.createClient(Util.getLocalHost(), server.getPort())) {
            String msg = "no replay";
            client.send(Util.stringToBytes(msg));
            assertEquals(0, client.getResponseHandler().numberOfOutstandingRequest());


            // make sure the request has already arrived
            long deadline = System.nanoTime() + TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
            while (System.nanoTime() < deadline && oneWayMsgs.size() == 0) {
                TimeUnit.MILLISECONDS.sleep(10);
            }

            assertEquals(1, oneWayMsgs.size());
            assertEquals(msg, oneWayMsgs.get(0));
        } catch (Exception e) {
            throw new RuntimeException("Invoke failed.", e);
        }
    }


    private void assertErrorsContain(Set<String> expected, Set<String> actual) {
        assertTrue(expected.size() == actual.size());
        Set<String> errors = Sets.newHashSet(expected);
        for (String error: actual) {
            boolean found = false;
            Iterator<String> iter = errors.iterator();
            while (iter.hasNext()) {
                String next = iter.next();
                if (error.contains(next)) {
                    iter.remove();
                    found = true;
                }
            }
            assertTrue("Could not find error contains " + error, found);
        }

        assertTrue(errors.isEmpty());
    }
}
