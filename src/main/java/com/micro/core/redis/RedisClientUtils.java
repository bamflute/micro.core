package com.micro.core.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.vertx.core.buffer.Buffer;
import io.vertx.redis.op.SetOptions;
import org.apache.servicecomb.foundation.vertx.VertxUtils;
import org.apache.servicecomb.foundation.vertx.client.ClientPoolFactory;
import org.apache.servicecomb.foundation.vertx.client.ClientPoolManager;
import org.apache.servicecomb.foundation.vertx.client.ClientVerticle;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;

public class RedisClientUtils
{
    private static ClientPoolManager<RedisClient> clientMgr;
    static boolean sync = true;

    public static void init(String vertxName, String redisHost, int redisPort, String redisPassword, int redisClientCount) throws InterruptedException
    {
        Vertx vertx = VertxUtils.getOrCreateVertxByName(vertxName, null);
        RedisOptions redisOptions = new RedisOptions()
                .setHost(redisHost)
                .setPort(redisPort)
                .setAuth(redisPassword);
        ClientPoolFactory<RedisClient> factory = (ctx) ->
        {
            return RedisClient.create(vertx, redisOptions);
        };
        clientMgr = new ClientPoolManager<>(vertx, factory);

        DeploymentOptions deployOptions = VertxUtils.createClientDeployOptions(clientMgr,redisClientCount);
        VertxUtils.blockDeploy(vertx, ClientVerticle.class, deployOptions);
    }


    public static void set(BufferIO value, int extime)
    {
        SetOptions options = new SetOptions().setEX(extime);
        RedisSession session = getRedisSession();
        Buffer buffer =  Buffer.buffer();
        value.writeToBuffer(buffer);
        session.setBinary(value.getKey(), buffer, options);
    }

    public static void set(BufferIO value)
    {
        RedisSession session = getRedisSession();
        Buffer buffer =  Buffer.buffer();
        value.writeToBuffer(buffer);
        session.setBinary(value.getKey(), buffer);
    }

    public static void hset(String mkey, String skey, String value)
    {
        getRedisSession().hset(mkey, skey, value);
    }

    public static void hdel(String mkey, String skey)
    {
        getRedisSession().hdel(mkey, skey);
    }

    public static int delete(String key)
    {
        RedisSession session = getRedisSession();
        session.delete(key);
        return 0;
    }

    public static String hget(String mkey, String skey)
    {
        return "";
    }

    public static int delete (List<String> keys)
    {
        RedisSession session = getRedisSession();
        session.delete(keys);
        return 0;
    }

    public static void get(String key, BufferIO bufferIO)
    {
        CompletableFuture<Buffer> future = new CompletableFuture<>();

        RedisSession session = getRedisSession();
        session.get(key, future);
        try
        {

            bufferIO.readFromBuffer(future.get());
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        catch (ExecutionException e)
        {
            e.printStackTrace();
        }
    }

    private static RedisSession getRedisSession()
    {
        RedisClient redisClient = clientMgr.findClientPool(sync);
        RedisSession session = new RedisSession(redisClient);
        return session;
    }


    public static void main(String[] args) throws Exception
    {
        TestCase testCase = new TestCase("111131", 123, 9999999);
        RedisClientUtils.init("momo", "192.168.1.104",6379, "hello", 10);
        RedisClientUtils.set(testCase);
        TestCase testCase1 = new TestCase();
        RedisClientUtils.get(testCase.getKey(), testCase1);
        System.out.println("testcase1" + testCase1.toString());

        TestCase testCase2 = new TestCase("111399", 123, 9999999);
        RedisClientUtils.set(testCase2,100);

        TestCase testCase3 = new TestCase();
        RedisClientUtils.get(testCase2.getKey(), testCase3);
        System.out.println("testcase3" + testCase3.toString());

        List<String> keys = new ArrayList<>();
        keys.add(testCase1.getKey());
        keys.add(testCase2.getKey());
        RedisClientUtils.delete(keys);
        TestCase testCase4 = new TestCase();
        RedisClientUtils.get(testCase2.getKey(), testCase4);
        System.out.println("testcase4" + testCase4.toString());

        TestCase testCase5 = new TestCase();
        RedisClientUtils.get(testCase1.getKey(), testCase5);
        System.out.println("testcase5" + testCase5.toString());
        System.out.println("hello world");
    }
}
