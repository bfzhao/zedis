package org.my.zedis;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.stereotype.Service;

@Service
@lombok.extern.slf4j.Slf4j
public class ZedisServer {
    @Value("${zedis.port}")
    private int port;

    private final RespChannelHandler respChannelHandler;

    public ZedisServer(RespChannelHandler respChannelHandler) {
        this.respChannelHandler = respChannelHandler;
    }

    public void run(String[] args) throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup(1);  // make sure it's single thread

        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new RespDecoder())
                                    .addLast(new RespEncoder())
                                    .addLast(respChannelHandler);
                        }
                    });

            ApplicationArguments applicationArguments = new DefaultApplicationArguments(args);
            if (applicationArguments.containsOption("zedis.port"))
                port = Integer.parseInt(applicationArguments.getOptionValues("zedis.port").get(0));

            ChannelFuture channelFuture = serverBootstrap.bind(port).sync();
            log.info("Zedis server started on port {}", port);
            channelFuture.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static String buildClientKey(ChannelHandlerContext ctx) {
        return ctx.channel().remoteAddress().toString();
    }
}
