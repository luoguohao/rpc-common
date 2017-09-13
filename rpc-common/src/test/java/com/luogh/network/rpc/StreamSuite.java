package com.luogh.network.rpc;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.luogh.network.rpc.buffer.FileSegmentManagedBuffer;
import com.luogh.network.rpc.buffer.ManagedBuffer;
import com.luogh.network.rpc.buffer.NioManagedBuffer;
import com.luogh.network.rpc.client.RpcResponseCallback;
import com.luogh.network.rpc.client.StreamReceivedCallback;
import com.luogh.network.rpc.client.TransportClient;
import com.luogh.network.rpc.common.*;
import com.luogh.network.rpc.server.TransportServer;
import com.luogh.network.rpc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author luogh
 */
public class StreamSuite {
    private static final String[] STREAMS = {"largeBuffer", "smallBuffer", "emptyBuffer", "file"};

    private static TransportServer server;
    private static TransportClientFactory factory;
    private static File testFile;
    private static File tempDir;

    private static ByteBuffer emptyBuffer;
    private static ByteBuffer smallBuffer;
    private static ByteBuffer largeBuffer;

    private static ByteBuffer createBuffer(int bufSize) {
        ByteBuffer buffer = ByteBuffer.allocate(bufSize);
        for (int i = 0; i < bufSize; i++) {
            buffer.put((byte)i);
        }
        buffer.flip();
        return buffer;
    }

    @BeforeClass
    public static void setup() throws Exception {
        tempDir = Files.createTempDir();
        emptyBuffer = createBuffer(0);
        smallBuffer = createBuffer(100);
        largeBuffer = createBuffer(100_000);

        testFile = File.createTempFile("stream-test-file", "txt", tempDir);
        FileOutputStream fp = new FileOutputStream(testFile);
        try {
            Random rand = new Random();
            for (int i = 0; i < 512; i++) {
                byte[] fileContent = new byte[1024];
                rand.nextBytes(fileContent);
                fp.write(fileContent);
            }
        } finally {
            fp.close();
        }

        final TransportConf conf = new TransportConf("test", new SystemPropertyConfigProvider());
        final StreamManager streamManager = new StreamManager() {
            @Override
            public ManagedBuffer getChunk(long streamId, int chunkIndex) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ManagedBuffer openStream(String streamId) {
                switch (streamId) {
                    case "largeBuffer":
                        return new NioManagedBuffer(largeBuffer);
                    case "smallBuffer":
                        return new NioManagedBuffer(smallBuffer);
                    case "emptyBuffer":
                        return new NioManagedBuffer(emptyBuffer);
                    case "file":
                        return new FileSegmentManagedBuffer(conf, testFile, 0, testFile.length());
                    default:
                        throw new IllegalStateException("Unknown StreamId: " + streamId);
                }
            }
        };

        final RpcHandler rpcHandler = new RpcHandler() {
            @Override
            public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
                throw new UnsupportedOperationException();
            }

            @Override
            public StreamManager streamManager() {
                return streamManager;
            }
        };
        final TransportContext context = new TransportContext(conf, rpcHandler);
        server = context.createServer();
        factory = context.createClientFactory();
    }


    @AfterClass
    public static void tearDown() throws Exception {
        if (server != null) {
            server.close();
        }
        factory.close();
        if (tempDir != null) {
            for (File f : tempDir.listFiles()) {
                f.delete();
            }
            tempDir.delete();
        }
    }

    @Test
    public void testZeroLengthStream() throws Throwable {
        TransportClient client = factory.createClient(Util.getLocalHost(), server.getPort());
        try {
            StreamTask streamTask = new StreamTask(client, "emptyBuffer", TimeUnit.SECONDS.toMillis(5));
            streamTask.run();
            streamTask.check();
        } finally {
            client.close();
        }
    }

    @Test
    public void testSingleStream() throws Throwable {
        TransportClient client = factory.createClient(Util.getLocalHost(), server.getPort());
        try {
            StreamTask streamTask = new StreamTask(client, "largeBuffer", TimeUnit.SECONDS.toMillis(5));
            streamTask.run();
            streamTask.check();
        } finally {
            client.close();
        }
    }

    @Test
    public void testMultipleStream() throws Throwable {
        TransportClient client = factory.createClient(Util.getLocalHost(), server.getPort());
        try {
            for (int i = 0; i < 20; i++) {
                StreamTask streamTask = new StreamTask(client, STREAMS[i % STREAMS.length],
                        TimeUnit.SECONDS.toMillis(5));
                streamTask.run();
                streamTask.check();
            }
        } finally {
            client.close();
        }
    }

    @Test
    public void testConcurrentStreams() throws Throwable {
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        TransportClient client = factory.createClient(Util.getLocalHost(), server.getPort());

        try {
            List<StreamTask> tasks = Lists.newArrayList();
            for (int i = 0; i < 20; i++) {
                StreamTask streamTask = new StreamTask(client, STREAMS[i % STREAMS.length],
                        TimeUnit.SECONDS.toMillis(5));
                tasks.add(streamTask);
                executorService.submit(streamTask);
            }

            executorService.shutdown();
            assertTrue("Timed out waiting for tasks.",
                    executorService.awaitTermination(30, TimeUnit.SECONDS));
            for (StreamTask task : tasks) {
                task.check();
            }
        } finally {
            executorService.shutdownNow();
            client.close();
        }
    }

    private static class StreamTask implements Runnable {

        private final TransportClient client;
        private final String streamId;
        private final long timeoutMs;
        private Throwable error;

        public StreamTask(TransportClient client, String streamId, long timeoutMs) {
            this.client = client;
            this.streamId = streamId;
            this.timeoutMs = timeoutMs;
        }

        @Override
        public void run() {
            ByteBuffer srcBuffer = null;
            OutputStream out = null;
            File outFile = null;
            try {
                ByteArrayOutputStream baos = null;
                switch (streamId) {
                    case "largeBuffer":
                        baos = new ByteArrayOutputStream();
                        out = baos;
                        srcBuffer = largeBuffer;
                        break;
                    case "smallBuffer":
                        baos = new ByteArrayOutputStream();
                        out = baos;
                        srcBuffer = smallBuffer;
                        break;
                    case "emptyBuffer":
                        baos = new ByteArrayOutputStream();
                        out = baos;
                        srcBuffer = emptyBuffer;
                        break;
                    case "file":
                        outFile = File.createTempFile("data", ".tmp", tempDir);
                        out = new FileOutputStream(outFile);
                        break;
                        default:
                            throw new IllegalStateException("Unknown Stream type." + streamId);
                }

                TestStreamCallback callback = new TestStreamCallback(out);
                client.stream(streamId, callback);
                waitForComplete(callback);

                if (srcBuffer == null) {
                    assertTrue("File stream did not match.", Files.equal(testFile, outFile));
                } else {
                    ByteBuffer base;
                    synchronized (srcBuffer) {
                        base = srcBuffer.duplicate();
                    }
                    byte[] result = baos.toByteArray();
                    byte[] expected = new byte[base.remaining()];
                    base.get(expected);
                    assertEquals(result.length, expected.length);
                    assertTrue("buffers don't match.", Arrays.equals(result, expected));
                }
            } catch (Throwable e) {
                error = e;
            } finally {
                if (out != null) {
                    try {
                        out.close();
                    } catch (Exception e) {

                    }
                }
                if (outFile != null) {
                    outFile.delete();
                }
            }
        }

        private void waitForComplete(TestStreamCallback callback) throws Exception {
            long curTime = System.currentTimeMillis();
            long deadLine = curTime + timeoutMs;
            synchronized (callback) {
                while (!callback.completed && curTime < deadLine) {
                    callback.wait(deadLine - curTime);
                    curTime = System.currentTimeMillis();
                }
            }

            assertTrue("Timed out waiting for stream.", callback.completed);
            assertNull(callback.error);

        }

        public void check() throws Throwable {
            if (error != null) {
                throw error;
            }
        }
    }

    private static final class TestStreamCallback implements StreamReceivedCallback {
        private final OutputStream outputStream;
        public volatile boolean completed;
        public volatile Throwable error;

        public TestStreamCallback(OutputStream outputStream) {
            this.outputStream = outputStream;
            this.completed = false;
        }

        @Override
        public void onDataReceived(String streamId, ByteBuffer data) throws IOException {
            byte[] tempData = new byte[data.remaining()];
            data.get(tempData);
            outputStream.write(tempData);
        }

        @Override
        public void onComplete(String streamId) throws IOException {
            outputStream.close();
            synchronized (this) {
                this.completed = true;
                this.notifyAll();
            }
        }

        @Override
        public void onFailure(String streamId, Throwable e) throws IOException {
            outputStream.close();
            error = e;
            synchronized (this) {
                this.completed = true;
                this.notifyAll();
            }
        }
    }
}
