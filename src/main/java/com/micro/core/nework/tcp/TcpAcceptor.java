package com.micro.core.nework.tcp;

import com.micro.core.nework.entity.NetEntity;
import com.micro.core.nework.tcp.method.ITcpAddPipeHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TcpAcceptor extends AbstractITcpProcessor
{
    private static Logger LOGGER = LogManager.getLogger();
    private ServerBootstrap bootstrap;
    private Channel acceptorChannel = null;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public TcpAcceptor(ITcpAddPipeHandler tcpAddPipeHandler)
    {
        bootstrap = new ServerBootstrap();
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();

        bootstrap.group(bossGroup, workerGroup)
                 .channel(NioServerSocketChannel.class)
                 .option(ChannelOption.TCP_NODELAY, true)
                 .option(ChannelOption.SO_REUSEADDR, true)
                 .option(ChannelOption.SO_RCVBUF, 10 * 1024)
                 .option(ChannelOption.SO_SNDBUF, 10 * 1024)
                 .option(EpollChannelOption.SO_REUSEPORT, true)
                 .childHandler(initPipeLine(tcpAddPipeHandler));
    }


    public ChannelFuture bind(NetEntity entity) throws Exception
    {
        ChannelFuture future = null;

        if (entity.getHost() == null)
        {
            future = bootstrap.bind(entity.getPort()).sync();
        }
        else
        {
            future = bootstrap.bind(entity.getHost(), entity.getPort()).sync();
        }

        future.addListener(startListener);
        acceptorChannel = future.channel();
        return future;
    }

    @Override
    public void close() throws Exception
    {
        super.close();
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        if (acceptorChannel != null)
        {
            ChannelFuture future = acceptorChannel.close();
            future.addListener(stopListener);
        }
        acceptorChannel = null;
    }

    public void loop()
    {
        try
        {
            acceptorChannel.closeFuture().sync();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    private ChannelInitializer<SocketChannel> initPipeLine(ITcpAddPipeHandler tcpAddPipeHandler)
    {
        return new ChannelInitializer<SocketChannel>()
        {

            @Override
            protected void initChannel(SocketChannel socketChannel) throws Exception
            {
                LOGGER.info("recv conn from "+socketChannel.remoteAddress().toString());
                ChannelPipeline pipeline = socketChannel.pipeline();
                pipeline.addLast("channelcollector", new ChannelCollector());
                tcpAddPipeHandler.addHandler(pipeline);
                //addChannel(socketChannel);
            }

        };
    }


    private ChannelFutureListener startListener = new ChannelFutureListener() {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
                LOGGER.info("SERVER_START_SUCCESSFULLY");
                System.out.println("SERVER_START_SUCCESSFULLY");
            } else {
                LOGGER.error("SERVER_START_FAILED");
            }
        }
    };

    private ChannelFutureListener stopListener = new ChannelFutureListener() {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
                System.out.println("SERVER_START_SUCCESSFULLY");
                LOGGER.info("SERVER_STOP_SUCCESSFULLY");
            } else {
                LOGGER.error("SERVER_STOP_FAILED");
            }
        }
    };

    private class ChannelCollector extends ChannelDuplexHandler
    {
        public void channelActive(ChannelHandlerContext ctx) throws Exception
        {
            // 把连接加入数组
            addChannel(ctx.channel());
            super.channelActive(ctx);
        }

        public void channelInactive(ChannelHandlerContext ctx) throws Exception
        {
            removeChannel(ctx.channel());
            ctx.fireChannelInactive();
        }
    }
}
