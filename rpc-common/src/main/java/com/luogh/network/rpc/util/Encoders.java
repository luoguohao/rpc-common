package com.luogh.network.rpc.util;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * @author luogh
 */
public class Encoders {

    public static class Strings {

        /** String encode length = length bytes + string bytes length **/
        public static int encodeLength(String s) {
            return 4 + s.getBytes(StandardCharsets.UTF_8).length;
        }

        public static void encode(ByteBuf bytebuf, String error) {
            byte[] bytes = error.getBytes(StandardCharsets.UTF_8);
            bytebuf.writeInt(bytes.length);
            bytebuf.writeBytes(bytes);
        }

        public static String decode(ByteBuf buf) {
            int length = buf.readInt();
            byte[] bytes = new byte[length];
            buf.readBytes(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }
}
