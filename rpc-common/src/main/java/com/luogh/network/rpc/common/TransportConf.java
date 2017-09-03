package com.luogh.network.rpc.common;

import com.google.common.primitives.Ints;
import com.luogh.network.rpc.util.Util;

/**
 * @author luogh
 */
public class TransportConf {

    private final String NETWORK_IO_MODE_KEY;
    private final String NETWORK_IO_PREFERDIRECTBUF_KEY;
    private final String NETWORK_IO_CONNECTIONTIMEOUT_KEY;
    private final String NETWORK_IO_BACKLOG_KEY;
    private final String NETWORK_IO_NUMCONNECTIONSPERPEER_KEY;
    private final String NETWORK_IO_SERVERTHREADS_KEY;
    private final String NETWORK_IO_CLIENTTHREADS_KEY;
    private final String NETWORK_IO_RECEIVEBUFFER_KEY;
    private final String NETWORK_IO_SENDERBUFFER_KEY;
    private final String NETWORK_SASL_TIMEOUT_KEY;
    private final String NETWORK_IO_MAXRETRIES_KEY;
    private final String NETWORK_IO_RETRYWAIT_KEY;
    private final String NETWORK_IO_LAZYFD_KEY;

    private final ConfigProvider conf;
    private final String module;

    public TransportConf(String module, ConfigProvider provider) {
        this.module = module;
        this.conf = provider;
        this.NETWORK_IO_MODE_KEY = getConfigKey("io.mode");
        this.NETWORK_IO_PREFERDIRECTBUF_KEY = getConfigKey("io.preferDirectBuf");
        this.NETWORK_IO_CONNECTIONTIMEOUT_KEY = getConfigKey("io.connectionTimeout");
        this.NETWORK_IO_BACKLOG_KEY = getConfigKey("io.backLog");
        this.NETWORK_IO_NUMCONNECTIONSPERPEER_KEY = getConfigKey("io.numConnectionsPerPeer");
        this.NETWORK_IO_SERVERTHREADS_KEY = getConfigKey("io.serverThreads");
        this.NETWORK_IO_CLIENTTHREADS_KEY = getConfigKey("io.clientThreads");
        this.NETWORK_IO_RECEIVEBUFFER_KEY = getConfigKey("io.receiveBuffer");
        this.NETWORK_IO_SENDERBUFFER_KEY = getConfigKey("io.senderBuffer");
        this.NETWORK_SASL_TIMEOUT_KEY = getConfigKey("sasl.timeout");
        this.NETWORK_IO_MAXRETRIES_KEY = getConfigKey("io.maxRetires");
        this.NETWORK_IO_RETRYWAIT_KEY = getConfigKey("io.retryWait");
        this.NETWORK_IO_LAZYFD_KEY = getConfigKey("io.lazyFD");
    }

    private String getConfigKey(String suffix) {
        return this.module + "." + suffix;
    }

    /**
     * Connection timeout in milliseconds. Default 120 secs.
     * @return
     */
    public int connectionTimeoutMs() {
        return (int)Util.timeStringAsMs(conf.get(this.NETWORK_IO_CONNECTIONTIMEOUT_KEY, "120s"));
    }

    /** IO mode: epoll or nio. **/
    public String ioMode() {
        return conf.get(this.NETWORK_IO_MODE_KEY, "nio").toUpperCase();
    }

    /** Number of threads used in the server thread pool. Default to 0, which is 2x#cores. **/
    public int serverThread() {
        return conf.getInt(this.NETWORK_IO_SERVERTHREADS_KEY, 0);
    }

    public String getModuleName() {
        return this.module;
    }

    /** If true, we will prefer allocating off-heap byte buffers within netty. **/
    public boolean preferDirectBuf() {
        return conf.getBoolean(this.NETWORK_IO_PREFERDIRECTBUF_KEY, false);
    }

    /** Requested maximum length of the queue of incoming connections. Default -1 for no backlog. **/
    public int backLog() {
        return conf.getInt(this.NETWORK_IO_BACKLOG_KEY, -1);
    }

    /**
     * Receive buffer size (SO_RCVBUF).
     * Note: the optimal size for receive buffer and send buffer should be
     * latency * network_bandwidth.
     * Assuming latecy = 1ms, network_bandwidth = 10Gbps
     * buffer size should be ~ 1.25MB
     * @return
     */
    public int receiveBuf() {
        return conf.getInt(this.NETWORK_IO_RECEIVEBUFFER_KEY, -1);
    }

    /**
     * Send buffer size (SO_SNDBUF).
     * @return
     */
    public int sendBuf() {
        return conf.getInt(this.NETWORK_IO_SENDERBUFFER_KEY, -1);
    }

    /**
     * Number of concurrent connections between two nodes for fetching data.
     * @return
     */
    public int numConnectionsPerPeer() {
        return conf.getInt(this.NETWORK_IO_NUMCONNECTIONSPERPEER_KEY, 1);
    }

    /**
     * Number of threads used in the client thread pool. Default to 0, which is 2x#cores.
     * @return
     */
    public int clientThreads() {
        return conf.getInt(this.NETWORK_IO_CLIENTTHREADS_KEY, 0);
    }

    /**
     * Minimum size of a block that we should start using memory map rather than reading in through
     * normal IO operations. This prevents from memory mapping very small blocks. In general, memory
     * mapping has high overhead for blocks close to or below the page size of the OS.
     * @return
     */
    public int memoryMapBytes() {
        return Ints.checkedCast(Util.byteStringAsBytes(conf.get("storage.memoryMapThreshold", "2m")));
    }

    /**
     * Whether to initialize FileDescriptor lazily or not. if true, file descriptors are created only when data is
     * going to be transferred, This can reduce the number of open files.
     * **/
    public boolean lazyFileDescriptor() {
        return conf.getBoolean(this.NETWORK_IO_LAZYFD_KEY, true);
    }
}
