package com.micro.core.process.service;

import com.micro.core.process.processor.AbstractRunnableProcessor;
import com.micro.core.process.processor.NewProcessorInterface;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * base class for process at a period
 */
public class ScheduleProcessStrategy
{
    private static Logger LOGGER = LogManager.getLogger();
    private ScheduledExecutorService scheduExecPool;
    private int threadNum = 5;
    private int maxQueueSize = 10000;
    private int start = 0;
    private int period = 1;
    BlockingQueue<Object> queue = null;
    private List<AbstractRunnableProcessor> processors = null;
    private List<ScheduledFuture<?>> scheduledFutures;


    public ScheduleProcessStrategy(String desc, int threadNum, int maxQueueSize)
    {
        this.threadNum = threadNum;
        this.maxQueueSize = maxQueueSize;
        processors = new ArrayList<AbstractRunnableProcessor>();
        if (maxQueueSize > 0)
        {
            queue = new ArrayBlockingQueue<Object>(maxQueueSize);
        }
        scheduExecPool = Executors.newScheduledThreadPool(2, new ProcessorsThreadFactory(desc));
    };


    public void start(NewProcessorInterface processorInterface, int delay, int period, int num)
    {
        if (period < 0 || delay < 0 || processorInterface == null)
        {
            LOGGER.error("input params error. pls check");
            return;
        }

        for (int i=0; i< num; i++)
        {
            start(processorInterface, delay, period);
        }
    }

    /**
     *
     * @param processorInterface: impl process for task, which will called
     * @param delay: delay time for exec task
     * @param period: period time
     */
    public void start(NewProcessorInterface processorInterface, int delay, int period)
    {
        if (period < 0 || delay < 0 || processorInterface == null)
        {
            LOGGER.error("input params error. pls check");
            return;
        }
        AbstractRunnableProcessor processor = processorInterface.newProcessor();
        if (queue != null)
        {
            processor.setBlockQueue(queue);
        }
        processor.start();
        ScheduledFuture<?> scheduledFuture;
        if (period == 0)
        {
            scheduledFuture = scheduExecPool.schedule(processor, delay, TimeUnit.MICROSECONDS);
        }
        else
        {
            scheduledFuture = scheduExecPool.scheduleAtFixedRate(processor, delay, period, TimeUnit.MICROSECONDS);
        }
        scheduledFutures.add(scheduledFuture);
        processors.add(processor);

        LOGGER.info("Started processing thread with queue size of " + maxQueueSize
                + " and " + threadNum + " threads.");
    }

    public void stop()
    {
        scheduledFutures.forEach(scheduledFuture -> {
            if (scheduledFuture != null && !scheduledFuture.isDone()) {
                scheduledFuture.cancel(true);
            }
        });

        scheduExecPool.shutdownNow();

        processors.forEach(processor -> processor.stop());
    }

    public void sendMessage(Object obj)
    {
        if (queue == null)
        {
            return;
        }

        try
        {
            queue.put(obj);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}
