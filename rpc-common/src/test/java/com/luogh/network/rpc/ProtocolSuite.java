package com.luogh.network.rpc;

import com.google.common.primitives.Ints;
import com.luogh.network.rpc.common.MessageDecoder;
import com.luogh.network.rpc.common.MessageEncoder;
import com.luogh.network.rpc.protocol.Message;
import com.luogh.network.rpc.protocol.RpcFailureMessage;
import com.luogh.network.rpc.protocol.RpcRequestMessage;
import com.luogh.network.rpc.protocol.RpcResponseMessage;
import com.luogh.network.rpc.util.ByteArrayWritableChannel;
import com.luogh.network.rpc.util.NettyUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.MessageToMessageEncoder;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author luogh
 */
@Slf4j
public class ProtocolSuite {

    private void testServerToClient(Message meg) {
        EmbeddedChannel clientChannel = new EmbeddedChannel(NettyUtil.createFrameDecoder(),new MessageDecoder());
        EmbeddedChannel serverChannel = new EmbeddedChannel(new FileRegionEncoder(), new MessageEncoder());

        serverChannel.writeOutbound(meg);
        while (!serverChannel.outboundMessages().isEmpty()) {
            Object object = serverChannel.readOutbound();
            clientChannel.writeInbound(object);
        }

        assertEquals(1, clientChannel.inboundMessages().size());
        assertEquals(meg, clientChannel.readInbound());
    }

    private void testClientToServer(Message msg) {
        EmbeddedChannel clientChannel = new EmbeddedChannel(new FileRegionEncoder(), new MessageEncoder());
        clientChannel.writeOutbound(msg);

        EmbeddedChannel serverChannel = new EmbeddedChannel(NettyUtil.createFrameDecoder(), new MessageDecoder());

        while (!clientChannel.outboundMessages().isEmpty()) {
            Object object = clientChannel.readOutbound();
            serverChannel.writeInbound(object);
        }

        assertEquals(1, serverChannel.inboundMessages().size());
        assertEquals(msg, serverChannel.readInbound());
    }

    @Test
    public void requests() {
        testClientToServer(new RpcRequestMessage(new TestManagedBuffer(10), 12345));
        testClientToServer(new RpcRequestMessage(new TestManagedBuffer(0), 12345));
    }

    @Test
    public void responses() {
        testServerToClient(new RpcResponseMessage(new TestManagedBuffer(0), 12345));
        testServerToClient(new RpcResponseMessage(new TestManagedBuffer(10), 12345));
        testServerToClient(new RpcFailureMessage( 12345, "connection refuse."));
    }


    /**
     * Handler to transform a FileRegion into a byte buffer. EmbeddedChannel doesn't actually transfer
     * bytes, but messages, so this is needed so that the frame decoder on the receiving side can
     * understand what MessageWithHeader actually contains.
     */
    private static class FileRegionEncoder extends MessageToMessageEncoder<FileRegion> {
        @Override
        protected void encode(ChannelHandlerContext ctx, FileRegion msg, List<Object> out) throws Exception {
            ByteArrayWritableChannel channel = new ByteArrayWritableChannel(Ints.checkedCast(msg.count()));
            byte[] buf = channel.getData();
            log.debug("buffer size:{}.", buf.length);
            while (msg.transferred() < msg.count()) {
                msg.transferTo(channel, msg.transferred());
            }
            out.add(Unpooled.wrappedBuffer(buf));
        }
    }
}
