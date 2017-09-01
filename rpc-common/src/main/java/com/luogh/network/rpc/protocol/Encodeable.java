package com.luogh.network.rpc.protocol;

import io.netty.buffer.ByteBuf;

/**
 * @author luogh
 */
public interface Encodeable {

    /** Number of bytes of the encoded form of this object **/
    int encodeLength();

    /** Serialized this object by writing into the given ByteBuf **/
    void encode(ByteBuf bytebuf);
}
