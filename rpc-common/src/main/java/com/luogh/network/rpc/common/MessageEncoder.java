package com.luogh.network.rpc.common;

import com.google.common.base.Preconditions;
import com.luogh.network.rpc.protocol.Message;
import com.luogh.network.rpc.protocol.MessageWithHeader;
import com.luogh.network.rpc.protocol.ResponseMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import lombok.extern.slf4j.Slf4j;

import java.awt.*;
import java.util.List;

/**
 * @author luogh
 */
@ChannelHandler.Sharable
@Slf4j
public class MessageEncoder extends MessageToMessageEncoder<Message> {

    /**
     * Encode messages. each message is encoded in a frame. and each message
     * has a head and body like the following description:
     *
     * |------------------- frame ------------------------------|
     * |    8 bytes   |     1 byte   |-------------| -----------|
     * | frame length | message type | head data   | body data  |
     *
     * NOTICE: some messages may not contain body, or the body data is not in the same frame.
     * **/
    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, List<Object> out) throws Exception {
        Preconditions.checkArgument(msg != null);
        Object body = null;
        boolean isBodyInFrame = false;
        long bodyLength = 0;

        if (msg.body() != null) {
            try {
                bodyLength = msg.body().size();
                body = msg.body().convertToNetty();
                isBodyInFrame = msg.isBodyInFrame();
            } catch (Exception e) {
                msg.body().release();
                if (msg instanceof ResponseMessage) {
                    ResponseMessage responseMessage = (ResponseMessage)msg;
                    String error = e.getMessage() != null ? e.getMessage() : "null";
                    log.error(String.format("Error processing %s for client %s.",
                            msg, ctx.channel().remoteAddress()), e);
                    encode(ctx, responseMessage.createFailureMessage(error), out);
                } else {
                    throw e;
                }
                return ;
            }
        }

        Message.Type type = msg.type();
        int headerLength = 8 +  type.encodeLength() + msg.encodeLength();
        long frameLength = headerLength + (isBodyInFrame ? bodyLength : 0);
        ByteBuf header = ctx.alloc().heapBuffer(headerLength);
        header.writeLong(frameLength);
        type.encode(header);
        msg.encode(header);

        assert header.writableBytes() == 0;

        if (body != null) {
            // We transfer ownership of the reference on msg.body to MessageWithHeader.
            // This reference will be freed when MessageWithHeader.deallocate() is called.
            out.add(new MessageWithHeader(msg.body(), header, body, bodyLength));
        } else {
            out.add(header);
        }

    }
}
