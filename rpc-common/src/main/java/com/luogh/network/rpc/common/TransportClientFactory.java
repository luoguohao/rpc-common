package com.luogh.network.rpc.common;

import com.luogh.network.rpc.client.TransportClient;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author luogh
 */
public interface TransportClientFactory extends Closeable {
    TransportClient createClient(String remoteHost, int remotePort) throws InterruptedException, IOException;
}
