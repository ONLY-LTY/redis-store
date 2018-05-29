package com.redis.store.proxy;

import redis.clients.jedis.JedisPoolConfig;

/**
 * Author LTY
 * Date 2018/05/24
 */
public class DefaultJedisPoolConfig {

    static final JedisPoolConfig CONFIG_LARGE = new JedisPoolConfig();

    static final JedisPoolConfig CONFIG_MEDIUM = new JedisPoolConfig();

    static final JedisPoolConfig CONFIG_SMALL = new JedisPoolConfig();

    static {
        CONFIG_LARGE.setMaxTotal(2000);
        CONFIG_LARGE.setMaxIdle(200);
        CONFIG_LARGE.setMinIdle(5);
        CONFIG_LARGE.setMaxWaitMillis(2000);
        CONFIG_LARGE.setBlockWhenExhausted(true);

        CONFIG_MEDIUM.setMaxTotal(100);
        CONFIG_MEDIUM.setMaxIdle(50);
        CONFIG_MEDIUM.setMinIdle(5);
        CONFIG_MEDIUM.setMaxWaitMillis(2000);
        CONFIG_MEDIUM.setBlockWhenExhausted(true);

        CONFIG_SMALL.setMaxTotal(5);
        CONFIG_SMALL.setMaxIdle(5);
        CONFIG_SMALL.setMinIdle(1);
        CONFIG_SMALL.setMaxWaitMillis(2000);
        CONFIG_SMALL.setBlockWhenExhausted(true);
    }
    private DefaultJedisPoolConfig(){}
}
