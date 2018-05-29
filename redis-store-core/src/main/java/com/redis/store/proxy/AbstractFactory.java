package com.redis.store.proxy;

import com.redis.store.cluster.Node;
import com.redis.store.dao.IRedisDao;
import com.redis.store.dao.IStoreDao;
import com.redis.store.executor.IExecutor;
import com.redis.store.monitor.RedisStoreMonitor;
import redis.clients.jedis.JedisPubSub;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Author LTY
 * Date 2018/05/23
 */
abstract class AbstractFactory implements InvocationHandler {

    private final RedisConfigStoreProxy configProxy;

    private final RedisInvocationListener redisInvocationListener;

    private final RedisStoreMonitor redisStoreMonitor;

    private static final Set<String> READ_METHODS;

    private static final Set<String> WRITE_METHODS;

    private static final ConcurrentMap<String, Method> METHOD_CACHE = new ConcurrentHashMap<>();

    static {
        Set<String> readMethods = new HashSet<>();

        readMethods.add("ttl");

        readMethods.add("exists");
        readMethods.add("getbit");
        readMethods.add("get");
        readMethods.add("mget");

        readMethods.add("hexists");
        readMethods.add("hget");
        readMethods.add("hmget");
        readMethods.add("hgetAll");
        readMethods.add("hkeys");
        readMethods.add("hlen");

        readMethods.add("lindex");
        readMethods.add("llen");
        readMethods.add("lrange");

        readMethods.add("scard");
        readMethods.add("sismember");
        readMethods.add("smembers");
        readMethods.add("srandmember");

        readMethods.add("zcard");
        readMethods.add("zcount");
        readMethods.add("zscore");
        readMethods.add("zrank");

        readMethods.add("zrangeByOffset");
        readMethods.add("zrangeByOffsetWithScores");
        readMethods.add("zrangeByScore");
        readMethods.add("zrangeWithScores");

        readMethods.add("zrevrange");
        readMethods.add("zrevrangeV2");
        readMethods.add("zrevrangeWithScore");
        readMethods.add("zrevrangeByScore");
        readMethods.add("zrevrangeByScoreWithScores");


        Set<String> writeMethods = new HashSet<>();
        for (Method method : IStoreDao.class.getMethods()) {
            if (!readMethods.contains(method.getName())) {
                writeMethods.add(method.getName());
            }
        }

        READ_METHODS = Collections.unmodifiableSet(readMethods);
        WRITE_METHODS = Collections.unmodifiableSet(writeMethods);
    }

    AbstractFactory(RedisConfigStoreProxy configProxy, RedisInvocationListener listener) {
        this.configProxy = configProxy;
        this.redisInvocationListener = listener;
        this.redisStoreMonitor = new RedisStoreMonitor(configProxy.getCluster().getName());
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        check(args);
        String methodName = method.getName();
        String hashKey = args[0].toString();
        Node node = configProxy.getNode(hashKey);
        if (READ_METHODS.contains(methodName)) {
            return handleReadMethod(hashKey, node, method, args);
        }
        if (WRITE_METHODS.contains(methodName)) {
            return handWriteMethod(hashKey, node, method, args);
        }
        throw new IllegalAccessException("Method [" + method.getName() + "] Not Exists");
    }

    private void check(Object[] args) {
        if (args != null && args.length > 0 && args[0] == null) {
            throw new NullPointerException("hashKey");
        }
    }

    private Object handleReadMethod(String hashKey, Node node, Method method, Object[] args) throws Throwable {
        Object returnValue = null;

        Object[] argsFrom = Arrays.copyOfRange(args, 1, args.length);
        Method redisMethod = getRedisMethod(method.getName(), argsFrom);

        List<IRedisDao> daoList = configProxy.getReadableRedisDaos(hashKey, node);
        for (int i = 0; i < daoList.size(); i++) {
            IRedisDao dao = daoList.get(i);

            long begin = System.currentTimeMillis();
            try {
                returnValue = redisMethod.invoke(dao, argsFrom);
                if (FailFast.getFailFastSwitch()) {
                    FailFast.succeed(dao.getHostPort());
                }
                if (redisInvocationListener != null) {
                    redisInvocationListener.onComplete(dao.getHostPort());
                }

                redisStoreMonitor.record(method.getName().toLowerCase(), argsFrom, returnValue, dao.getHostPort(), (int) (System.currentTimeMillis() - begin));
                return returnValue;
            } catch (Exception e) {
                if (FailFast.getFailFastSwitch()) {
                    FailFast.fail(dao.getHostPort());
                }
                if (i == daoList.size() - 1) {
                    throw e;
                }
            }
        }
        return returnValue;
    }

    private Object handWriteMethod(String hashKey, Node node, Method method, Object[] args) throws Throwable {
        Object returnValue = null;

        Object[] argsFrom = Arrays.copyOfRange(args, 1, args.length);
        Method redisMethod = getRedisMethod(method.getName(), argsFrom);

        List<IRedisDao> daoList = this.configProxy.getShardWriteDao(hashKey);
        for (int i = 0; i < daoList.size(); i++) {
            long begin = System.currentTimeMillis();

            IRedisDao dao = daoList.get(i);
            try {
                returnValue = redisMethod.invoke(dao, argsFrom);
                if (FailFast.getFailFastSwitch()) {
                    FailFast.succeed(dao.getHostPort());
                }
                if (redisInvocationListener != null) {
                    redisInvocationListener.onComplete(dao.getHostPort());
                }
            } catch (Exception e) {
                if (FailFast.getFailFastSwitch()) {
                    FailFast.fail(dao.getHostPort());
                }
                throw e;
            } finally {
                redisStoreMonitor.record(method.getName().toLowerCase(), argsFrom, returnValue, dao.getHostPort(), (int) (System.currentTimeMillis() - begin));
            }
        }
        return returnValue;
    }

    private Method getRedisMethod(String nameFrom, Object[] argsFrom) throws Exception {
        StringBuilder methodSignBuilder = new StringBuilder(nameFrom);
        Class[] argsTypeFrom = new Class[argsFrom.length];
        for (int i = 0; i < argsFrom.length; i++) {
            Class<?> argClass = argsFrom[i].getClass();
            methodSignBuilder.append(argClass.getSimpleName());

            if (argClass.isPrimitive()) {
                argsTypeFrom[i] = argClass;
                continue;
            }
            if (argClass == Integer.class) {
                argsTypeFrom[i] = int.class;
            } else if (argClass == Long.class) {
                argsTypeFrom[i] = long.class;
            } else if (argClass == Double.class) {
                argsTypeFrom[i] = double.class;
            } else if (argClass == Boolean.class) {
                argsTypeFrom[i] = boolean.class;
            } else if (argsFrom[i] instanceof Map) {
                argsTypeFrom[i] = Map.class;
            } else if (argsFrom[i] instanceof IExecutor) {
                argsTypeFrom[i] = IExecutor.class;
            } else if (argsFrom[i] instanceof List) {
                argsTypeFrom[i] = List.class;
            } else if (argsFrom[i] instanceof JedisPubSub) {
                argsTypeFrom[i] = JedisPubSub.class;
            } else {
                argsTypeFrom[i] = argClass;
            }
        }

        String methodSign = methodSignBuilder.toString();
        Method method = METHOD_CACHE.get(methodSign);
        if (method == null) {
            method = IRedisDao.class.getMethod(nameFrom, argsTypeFrom);
            Method _method = METHOD_CACHE.putIfAbsent(methodSign, method);
            if (_method != null) {
                method = _method;
            }
        }
        return method;
    }
}
