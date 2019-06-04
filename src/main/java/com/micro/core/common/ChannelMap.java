package com.micro.core.common;

import io.netty.channel.Channel;

import java.util.HashMap;
import java.util.Map;

public class ChannelMap
{
    Map<String, ChannelList> channelMap = new HashMap<>();

    public void put(String workerId, Channel channel)
    {
        ChannelList channels = channelMap.get(workerId);
        if (channels == null)
        {
            channels = new ChannelList();
            channelMap.put(workerId, channels);
        }
        channels.add(channel);
    }

    public void remove(String workerId)
    {
        channelMap.remove(workerId);
    }

    public void remove(String workerId, Channel channel)
    {
        ChannelList channels = channelMap.get(workerId);
        if (channels == null)
        {
            return;
        }
        if (channels.getCount() > 0)
        {
            channels.remove(channel);
        }

        if (channels.getCount() < 1 || channels == null)
        {
            remove(workerId);
        }
    }

    public Channel getChannel(String workerId)
    {
        ChannelList channels = channelMap.get(workerId);
         if (null == channelMap || channels.getCount() < 1)
         {
             return  null;
         }

         return channels.fetch();
    }

}
