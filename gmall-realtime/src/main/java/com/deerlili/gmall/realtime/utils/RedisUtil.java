package com.deerlili.gmall.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * RedisUtil redis util
 *
 * @author lixx
 * @date 2022/7/26 14:11
 */
public class RedisUtil {

    public static JedisPool jedisPool = null;

    public static Jedis getJedis() {
        if (jedisPool == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

            // 最大可用连接数
            jedisPoolConfig.setMaxTotal(100);
            // 连接耗尽是否等待
            jedisPoolConfig.setBlockWhenExhausted(true);
            // 等待时间
            jedisPoolConfig.setMaxWaitMillis(2000);
            // 最大闲置连接数
            jedisPoolConfig.setMaxIdle(5);
            // 最小闲置连接数
            jedisPoolConfig.setMinIdle(5);
            // 取连接的时候进程测试ping pong
            jedisPoolConfig.setTestOnBorrow(true);

            jedisPool = new JedisPool(jedisPoolConfig, "hadoop102", 6379, 1000);

            System.out.println("开辟连接池");

            return jedisPool.getResource();
        } else {
            System.out.println("连接池："+jedisPool.getNumActive());
            return jedisPool.getResource();
        }
    }
}
