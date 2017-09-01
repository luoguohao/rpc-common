package com.luogh.network.rpc.common;

/**
 * @author luogh
 */
public class TransportConf {

    public int connectionTimeoutMs() {
        return 0;
    }

    public String ioMode() {
        return null;
    }

    public int serverThread() {
        return -1;
    }

    public String getModuleName() {
        return null;
    }

    public boolean preferDirectBufs() {
        return false;
    }

    public int backLog() {
        return -1;
    }

    public int receiveBuf() {
        return -1;
    }

    public int sendBuf() {
        return -1;
    }

    public int numConnectionsPerPeer() {
        return 1;
    }

    public int clientThreads() {
        return 1;
    }

    public long memoryMapBytes() {
        return 1;
    }

    public boolean lazyFileDescriptor() {
        return false;
    }
}
