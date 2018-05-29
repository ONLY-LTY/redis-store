package com.redis.store.proxy;

import com.redis.store.dao.IStoreDao;
import redis.clients.jedis.JedisPoolConfig;

import java.lang.reflect.Proxy;

import static com.redis.store.proxy.DefaultJedisPoolConfig.CONFIG_MEDIUM;


public class StoreDaoFactory extends AbstractFactory {

    private StoreDaoFactory(RedisConfigStoreProxy configProxy) {
        this(configProxy, RedisInvocationListener.EMPTY);
    }

    private StoreDaoFactory(RedisConfigStoreProxy configProxy, RedisInvocationListener listener) {
        super(configProxy, listener);
    }

    public static IStoreDao createStoreDao(String clusterName) {
        return createStoreDao(clusterName, CONFIG_MEDIUM, RedisInvocationListener.EMPTY);
    }

    public static IStoreDao createStoreDao(String clusterName, JedisPoolConfig jedisPoolConfig, RedisInvocationListener listener) {
        RedisConfigStoreProxy configProxy = RedisConfigStoreProxy.getProxyInstance(clusterName, jedisPoolConfig, 0);
        return (IStoreDao) Proxy.newProxyInstance(StoreDaoFactory.class.getClassLoader(), new Class[]{IStoreDao.class}, new StoreDaoFactory(configProxy, listener));
    }
}
