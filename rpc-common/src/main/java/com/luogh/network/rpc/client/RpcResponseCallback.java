package com.luogh.network.rpc.client;

import java.nio.ByteBuffer;

/**
 * @author luogh
 */
public interface RpcResponseCallback {

    void onSuccess(ByteBuffer message);

    void onFailure(Throwable e);
}
