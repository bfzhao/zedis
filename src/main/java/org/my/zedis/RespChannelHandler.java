package org.my.zedis;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.my.ConnectionManager;
import org.springframework.stereotype.Component;

import static org.my.zedis.ZedisServer.buildClientKey;

@lombok.extern.slf4j.Slf4j
@Component()
@ChannelHandler.Sharable
public class RespChannelHandler extends ChannelInboundHandlerAdapter implements ChannelHandler {
    private final HandlerRegistry handlerRegistry;
    private final ConnectionManager connectionManager;

    public RespChannelHandler(HandlerRegistry handlerRegistry, ConnectionManager connectionManager) {
        this.handlerRegistry = handlerRegistry;
        this.connectionManager = connectionManager;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RespType) {
            RespType respType = (RespType) msg;
            if (respType.getType() == RespType.Type.Arrays) {
                RespType[] list = respType.asArray();
                if (list.length > 0) {
                    int offset = 1;
                    String name = list[0].asString().toUpperCase();
                    RedisCommandHandler handler = handlerRegistry.getHandler(name);
                    if (handler == null && list.length > 1) {
                        name += " " + list[1].asString().toUpperCase();
                        handler = handlerRegistry.getHandler(name);
                        offset++;
                    }

                    if (handler != null) {
                        RespType[] args = new RespType[list.length - offset];
                        System.arraycopy(list, offset, args, 0, args.length);

                        String clientKey = buildClientKey(ctx);
                        RespType ret = handler.handle(name, args, clientKey, connectionManager.getDb(clientKey));
                        ctx.writeAndFlush(ret);
                    } else {
                        ctx.writeAndFlush(RespType.ofError("unsupported command: " + name));
                    }
                }
            }
        } else {
            log.warn("Received unknown message type");
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("exception: ", cause);
        ctx.close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        connectionManager.register(buildClientKey(ctx));
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        connectionManager.remove(buildClientKey(ctx));
        super.channelInactive(ctx);
    }
}
