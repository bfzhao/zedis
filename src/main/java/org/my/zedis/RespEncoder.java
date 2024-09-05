package org.my.zedis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;

public class RespEncoder extends MessageToByteEncoder<Object> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws IOException {
        if (msg instanceof RespType) {
            RespType respType = (RespType) msg;
            out.writeBytes(respType.encode());
        } else {
            // Handle other types accordingly
            throw new IllegalArgumentException("Unsupported message type for encoding: " + msg.getClass());
        }
    }
}
