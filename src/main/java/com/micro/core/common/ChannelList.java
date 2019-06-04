package com.micro.core.common;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ChannelList
{
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private List<Channel> collection = new ArrayList<Channel>();

    public Channel[] getall()
    {
        return collection.toArray(new Channel[0]);
    }

    public Channel fetch()
    {

        try
        {
            lock.readLock().lock();
            int size = collection.size();
            if (size == 0)
            {
                return null;
            }

            int idx = (int) getNextAtomicValue(indexSeq, Limited);
            Channel ret = collection.get(idx % size);
            // 超过65535归0
            return ret;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public boolean add(Channel ele)
    {

        boolean r = false;
        try
        {
            lock.writeLock().lock();
            r = collection.add(ele);
        }
        finally
        {
            lock.writeLock().unlock();
        }
        return r;
    }

    public boolean remove(Channel ele)
    {

        boolean r = false;
        try
        {
            lock.writeLock().lock();
            r = collection.remove(ele);
        }
        finally
        {
            lock.writeLock().unlock();
        }
        return r;
    }

    public int  getCount()
    {
        return collection.size();
    }

    private  long getNextAtomicValue(AtomicLong atomicObj,long limited)
    {
        long ret = atomicObj.getAndIncrement();

        if (ret > limited)
        {
            synchronized (atomicObj)
            {
                //双重判断，只能有一个线程更新值
                if (atomicObj.get() > limited)
                {
                    atomicObj.set(0);
                    return 0;
                }
                else
                {
                    return atomicObj.getAndIncrement();
                }
            }
        }
        else
        {
            return ret;
        }
    }

    private final static long Limited = 65535L;
    private AtomicLong indexSeq = new AtomicLong();
}
