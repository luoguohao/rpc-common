package com.luogh.network.rpc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.luogh.network.rpc.client.TransportClient;
import com.luogh.network.rpc.common.*;
import com.luogh.network.rpc.server.TransportServer;
import com.luogh.network.rpc.util.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertNotSame;
import static junit.framework.TestCase.*;

/**
 * @author luogh
 */
public class TransportClientFactorySuite {
    private TransportConf conf;
    private TransportContext context;
    private TransportServer server1;
    private TransportServer server2;
    private TransportClientFactory factory;


    @Before
    public void init() {

        conf = new TransportConf("test", new SystemPropertyConfigProvider());
        RpcHandler rpcHandler = new NoOpRpcHandler();
        context = new TransportContext(conf, rpcHandler);
        factory = context.createClientFactory();
        server1 = context.createServer();
        server2 = context.createServer();
    }

    @After
    public void shutdown() throws Exception {
        if (factory != null) {
            factory.close();
        }
        if (server1 != null) {
            server1.close();
        }

        if (server2 != null) {
            server2.close();
        }
    }

    private void testCientReuse(int numOfConnection, boolean concurrent) throws Exception {
        Map<String, String> properties = ImmutableMap.of(
                "test.io.numConnectionsPerPeer",  Integer.toString(numOfConnection),
                "test.io.connectionTimeout", "10s"
        );
        TransportConf config = new TransportConf("test", new MapConfigProvider(properties));
        RpcHandler handler = new NoOpRpcHandler();
        TransportContext context = new TransportContext(config, handler);
        TransportClientFactory factory = context.createClientFactory();
        Thread[] threads = new Thread[numOfConnection * 10];
        final AtomicInteger failedCnt = new AtomicInteger(0);
        final Set<TransportClient> clients = Sets.newConcurrentHashSet();

         for (int i = 0; i < threads.length; i++) {
             threads[i] = new Thread(() -> {
                 try {
                     TransportClient client = factory.createClient(Util.getLocalHost(), server1.getPort());
                     assertTrue(client.isAlive()); // returned client must be alive
                     clients.add(client);
                 } catch (Exception e) {
                     failedCnt.incrementAndGet();
                     e.printStackTrace();
                 }
             });

             if (concurrent) {
                 threads[i].start();
             } else {
                 threads[i].run();
             }
         }

         // wait until all thread completes.
        for (Thread t : threads) {
             t.join();
        }

        assertEquals(numOfConnection, clients.size());
        assertEquals(0, failedCnt.get());

        for (TransportClient client : clients) {
            client.close();
        }

        factory.close();
    }

    @Test
    public void reuseClientsUpToConfigVariable() throws Exception {
        testCientReuse(2, false);
        testCientReuse(4, false);
        testCientReuse(8, false);
    }

    @Test
    public void reuseClientsUpToConfigVariableConcurrent() throws Exception {
        testCientReuse(2, true);
        testCientReuse(4, true);
        testCientReuse(8, true);
    }

    @Test
    public void returnDifferentClientsForDifferentServers() throws Exception {
        TransportClient client1 = factory.createClient(Util.getLocalHost(), server1.getPort());
        TransportClient client2 = factory.createClient(Util.getLocalHost(), server2.getPort());

        assertTrue(client1.isAlive());
        assertTrue(client2.isAlive());
        assertNotSame(client1, client2);
    }

    @Test
    public void neverReturnInactiveClients() throws Exception {
        TransportClient client = factory.createClient(Util.getLocalHost(), server1.getPort());
        client.close();

        //wait client to close
        long curTime = System.currentTimeMillis();
        while (client.isAlive() && System.currentTimeMillis() - curTime < 3000) {
            Thread.sleep(10);
        }

        TransportClient client1 = factory.createClient(Util.getLocalHost(), server1.getPort());

        assertTrue(client1.isAlive());
        assertNotSame(client1, client);
    }

    @Test
    public void closeBlockClientsWithFactory() throws Exception {
        TransportClient client = factory.createClient(Util.getLocalHost(), server1.getPort());
        TransportClient client1 = factory.createClient(Util.getLocalHost(), server1.getPort());
        assertSame(client, client1);
        assertTrue(client.isAlive());
        assertTrue(client1.isAlive());
        factory.close();
        assertFalse(client.isAlive());
        assertFalse(client1 .isAlive());
    }

    @Test
    public void closeIdleConnectionForRequestTimeOut() throws Exception {
        TransportConf conf = new TransportConf("test",
                new MapConfigProvider(ImmutableMap.of("test.io.connectionTimeout", "1s")));
        TransportContext context = new TransportContext(conf, new NoOpRpcHandler(), true);
        TransportClientFactory factory = context.createClientFactory();
        TransportClient client = factory.createClient(Util.getLocalHost(), server1.getPort());
        assertTrue(client.isAlive());
        long expireTime = System.currentTimeMillis() + 10000; // 10s
        while (client.isAlive() && System.currentTimeMillis() < expireTime) {
            Thread.sleep(10);
        }
        assertFalse(client.isAlive());
        factory.close();
    }
}
