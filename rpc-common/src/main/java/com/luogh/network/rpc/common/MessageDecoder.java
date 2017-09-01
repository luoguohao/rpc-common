package com.luogh.network.rpc.common;

import com.luogh.network.rpc.protocol.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author luogh
 */

@ChannelHandler.Sharable
@Slf4j
public class MessageDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        Message.Type type = Message.Type.decode(msg);
        Message message = decode(type, msg);
        assert message.type() == type;
        log.trace("Received message {}:{}.", type, message);
        out.add(message);
    }

    private Message decode(Message.Type type, ByteBuf msg) {
        switch (type) {
            case ChunkFetchRequest:
                return ChunkFetchRequestMessage.decode(msg);
            case RpcFailure:
                return RpcFailureMessage.decode(msg);
            case RpcRequest:
                return RpcRequestMessage.decode(msg);
            case RpcResponse:
                return RpcResponseMessage.decode(msg);
            case OneWayMessage:
                return OneWayMessage.decode(msg);
            case StreamFailure:
                return StreamFailure.decode(msg);
            case StreamRequest:
                return StreamRequest.decode(msg);
            case StreamResponse:
                return StreamResponse.decode(msg);
            case ChunkFetchFailure:
                return ChunkFetchFailure.decode(msg);
            case ChunkFetchSuccess:
                return ChunkFetchSuccess.decode(msg);
                default:
                    throw new IllegalArgumentException("Invalid message type={}." + type);
        }
    }
}
