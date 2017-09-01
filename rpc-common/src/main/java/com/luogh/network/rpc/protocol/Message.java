package com.luogh.network.rpc.protocol;

import com.luogh.network.rpc.buffer.ManagedBuffer;
import io.netty.buffer.ByteBuf;

/**
 * @author luogh
 */
public interface Message extends Encodeable {

    /** Used to identify the message type. **/
    Type type();

    /** An optional body for the message. **/
    ManagedBuffer body();

    /** Whether to include the body of the message in the same frame as the message. **/
    boolean isBodyInFrame();



    enum Type implements Encodeable {

        ChunkFetchRequest(0), ChunkFetchSuccess(1), ChunkFetchFailure(2),
        StreamRequest(3), StreamResponse(4), StreamFailure(5),
        RpcRequest(6), RpcResponse(7), RpcFailure(8),
        OneWayMessage(9), User(-1);

        private final byte type;

        Type(int type) {
            assert type < 128 : "can not have more than 128 message types.";
            this.type = (byte)type;
        }

        public int getType() {
            return this.type;
        }

        @Override
        public int encodeLength() {
            return 1;
        }

        @Override
        public void encode(ByteBuf bytebuf) {
            bytebuf.writeByte(type);
        }

        public static Type decode(ByteBuf byteBuf) {
            byte type = byteBuf.readByte();
            switch (type) {
                case 0 : return ChunkFetchRequest;
                case 1 : return ChunkFetchSuccess;
                case 2 : return ChunkFetchFailure;
                case 3 : return StreamRequest;
                case 4 : return StreamResponse;
                case 5 : return StreamFailure;
                case 6 : return RpcRequest;
                case 7 : return RpcResponse;
                case 8 : return RpcFailure;
                case 9 : return OneWayMessage;
                case -1 : throw new IllegalArgumentException("User type messages cannot be decoded.");
                default : throw new IllegalArgumentException("Unknown type message id:" + type);
            }
        }
    }
}
