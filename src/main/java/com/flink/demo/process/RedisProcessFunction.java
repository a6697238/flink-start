package com.flink.demo.process;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

/**
 * 功能描述：
 *
 * @Author: winghou
 * @Date: 2021/7/27 3:17 下午
 */
public class RedisProcessFunction extends ProcessFunction<String,String> {

    private AtomicInteger timerRun = new AtomicInteger(0);

    private String currentTime;

    private JedisSentinelPool pool;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(10);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMinIdle(5);

        Set<String> sentinels = new HashSet<String>(Arrays.asList(
                "9.134.124.180:63791",
                "9.134.124.180:63791",
                "9.134.124.180:63791"
        ));

        pool = new JedisSentinelPool("master-1", sentinels, jedisPoolConfig, "qzone");



    }


    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        Jedis jedis = pool.getResource();

        jedis.setnx("mykey".getBytes(), "myvalue".getBytes());
        String myvalue = new String(jedis.get("mykey".getBytes()));

        System.out.println(myvalue);

    }




}
