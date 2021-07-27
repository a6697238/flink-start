package redis;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

/**
 * 功能描述：
 *
 * @Author: winghou
 * @Date: 2021/7/27 3:42 下午
 */
public class RedisTest {

    public static void main(String[] args) {
        redisConnect();
    }

    public static void redisConnect(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(10);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMinIdle(5);

        Set<String> sentinels = new HashSet<String>(Arrays.asList(
                "9.134.124.180:63791",
                "9.134.124.180:63791",
                "9.134.124.180:63791"
        ));

        JedisSentinelPool pool = new JedisSentinelPool("master-1", sentinels, jedisPoolConfig, "qzone");

        Jedis jedis = pool.getResource();

        System.out.println(jedis.setnx("my2key".getBytes(), "myvalue".getBytes()));
        String myvalue = new String(jedis.get("mykey".getBytes()));

        System.out.println(myvalue);
    }

}
