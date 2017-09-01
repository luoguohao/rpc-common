package com.luogh.network.rpc.protocol;

/**
 * @author luogh
 */
public interface ResponseMessage extends Message {
    ResponseMessage createFailureMessage(String error);
}
