package com.micro.core.redis;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.redis.RedisClient;
import io.vertx.redis.op.SetOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RedisSession
{
    RedisClient redis;

    private static Logger LOGGER = LogManager.getLogger();


    public RedisSession(RedisClient redis)
    {
        this.redis = redis;
    }

    public void delete(String key)
    {
        redis.del(key, res->{
            if (res.succeeded())
            {
                LOGGER.debug("success to delete key: {} " + key);
            }
            else
            {
                LOGGER.debug("failed to delete key: {} " + key);
            }
        });
    }

    public void delete(List<String> keys)
    {
        redis.delMany(keys, res->{
            if (res.succeeded())
            {
                LOGGER.debug("success to delete key: {} " + keys.toString());
            }
            else
            {
                LOGGER.debug("failed to delete key: {} " + keys.toString());
            }
        });
    }

    public void setBinary(String key, Buffer buffer, SetOptions options)
    {
        redis.setBinaryWithOptions(key, buffer, options, res->{
            if (res.succeeded())
            {
                LOGGER.info("success to set key: {} " + key + " data:" + buffer.toString());
            }
            else
            {
                LOGGER.info("failed to set key: {} " + key + " data:" + buffer.toString());
            }
        });
    }

    public void setBinary(String key, Buffer buffer)
    {
        redis.setBinary(key, buffer, res->{
            if (res.succeeded())
            {
                LOGGER.info("success to set key: " + key + " data:" + buffer.toString());
            }
            else
            {
                LOGGER.info("failed to set key: " + key + " data:" + buffer.toString());
            }
        });
    }

    public void hset(String mkey, String skey, String value)
    {
        redis.hset(mkey, skey, value, res -> {
            if (res.succeeded())
            {
                LOGGER.info("success to set key:" + mkey + " key:" + skey + " value:" + value);
            }
            else
            {
                LOGGER.info("failed to set key:" +  mkey + " key:" + skey + " value:" + value);
            }
        });
    }

    public void hdel(String mkey, String skey)
    {
        redis.hdel(mkey, skey, res -> {
            if (res.succeeded())
            {
                LOGGER.info("success to set key:" + mkey + " key:" + skey);
            }
            else
            {
                LOGGER.info("failed to set key:" +  mkey + " key:" + skey);
            }
        });
    }

    public void get(String key, CompletableFuture<Buffer> bufferFuture)
    {
        redis.getBinary(key, res ->
                        {
                            if (res.succeeded())
                            {
                                LOGGER.info(res.result());
                                bufferFuture.complete(res.result());
                                return;
                            }

                            bufferFuture.completeExceptionally(res.cause());
                        });
    }
}
