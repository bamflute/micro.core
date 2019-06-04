package com.micro.core.nework.tcp.method;

import io.netty.channel.ChannelPipeline;

public interface ITcpAddPipeHandler
{
    public void addHandler(ChannelPipeline pipeline);
}
