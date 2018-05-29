package com.redis.store.dao;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import com.redis.store.executor.IExecutor;
import com.redis.store.util.JsonUtils;
import com.redis.store.utils.StringUtils;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.SafeEncoder;
import redis.clients.util.Slowlog;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Author LTY
 * Date 2018/05/23
 */
public class SimpleRedisDao implements IRedisDao {

    private static final int SUBSCRIBE_RETRY_MAX_INTERVAL = 3000;

    private static final Logger LOGGER = Logger.getLogger(SimpleRedisDao.class);

    private HostPort hostport;

    private volatile JedisPool jedisPool;

    private volatile boolean close;

    private Map<JedisPubSub, String[]> jedisPubSubMap = Collections.synchronizedMap(new HashMap<JedisPubSub, String[]>());

    public SimpleRedisDao(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public String getHostPort() {
        return hostport.toString();
    }

    @Override
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public SimpleRedisDao setHostPort(HostPort hostport) {
        this.hostport = hostport;
        return this;
    }

    @Override
    public boolean ping() {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            String result = jedis.ping();
            return "PONG".equalsIgnoreCase(result);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.ping() error", e);
            return false;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Set<String> keys(String pattern) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.keys(pattern);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("keys(" + pattern + ")", e);
            return null;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long persist(byte[] key) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.persist(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("persist(" + str(key) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long pexpire(byte[] key, int milliseconds) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.pexpire(key, milliseconds);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("pexpire(" + str(key) + ", " + milliseconds + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long pexpireAt(byte[] key, long millisecondsTimestamp) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.pexpireAt(key, millisecondsTimestamp);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("pexpireAt(" + str(key) + ", " + millisecondsTimestamp + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long pttl(byte[] key) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.pttl(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("pttl(" + str(key) + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    @Deprecated
    public void destroy() {
        jedisPool.destroy();
        close = true;
    }

    @Override
    public boolean isDestroyed() {
        return close;
    }

    @Override
    public byte[] brpoplpush(byte[] key, byte[] dest, int timeout) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.brpoplpush(key, dest, timeout);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("brpoplpush(" + new String(key) + "," + new String(dest) + "," + timeout + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    @Override
    public byte[] rpoplpush(byte[] key, byte[] dest) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.rpoplpush(key, dest);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("rpoplpush(" + new String(key) + "," + new String(dest) + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    @Override
    public List<String> blpop(String key, int timeout) {
        List<String> result;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            result = jedis.blpop(timeout, key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.blpop() error, key: " + key, e);
            throw new RuntimeException("redis blpop error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return result;
    }

    @Override
    public List<String> blpop(int timeout, String... keys) {
        List<String> result;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            result = jedis.blpop(timeout, keys);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.blpop() error, keys: " + Arrays.toString(keys), e);
            throw new RuntimeException("redis blpop error, keys = " + Arrays.toString(keys), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return result;
    }

    @Override
    public Long del(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.del(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.del() error, key: {0}" + key, e);
            throw new RuntimeException("redis del error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long del(String... keys) {
        Jedis jedis = null;
        JedisPool pool = getJedisPool();
        try {
            jedis = pool.getResource();
            return jedis.del(keys);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis.del(" + Arrays.toString(keys) + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public byte[] dump(byte[] key) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.dump(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("dump(" + str(key) + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public boolean exists(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.exists(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.exists() error, key: {0}" + key, e);
            throw new RuntimeException("redis exist error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String rename(String key, String newkey) {

        String result;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            result = jedis.rename(key, newkey);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error(StringUtils.format("SimpleRedisDao.rename() error, key: {0}, newKey: {1}.", key, newkey), e);
            throw new RuntimeException("redis rename error, key = " + key + ", newKey = " + newkey, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return result;
    }

    @Override
    public Long renamenx(String key, String newkey) {

        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.renamenx(key, newkey);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error(StringUtils.format("SimpleRedisDao.renamenx() error, key: {0}, newKey: {1}.", key, newkey), e);
            throw new RuntimeException("redis renamenx error, key = " + key + ", newKey = " + newkey, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String restore(byte[] key, int ttl, byte[] serializedValue) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.restore(key, ttl, serializedValue);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("restore(" + str(key) + ", " + ttl + ", " + Arrays.toString(serializedValue) + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public List<String> sort(String key) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.sort(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("sort(" + key + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public List<String> sort(String key, SortingParams sortingParams) {
        Jedis jedis = null;

        //noinspection Duplicates
        try {
            jedis = getJedisResource();
            return jedis.sort(key, sortingParams);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("sort(" + key + ", " + sortingParams + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long sort(String key, SortingParams sortingParams, String dstkey) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.sort(key, sortingParams, dstkey);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("sort(" + key + ", " + sortingParams + ", " + dstkey + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long sort(String key, String dstkey) {
        Jedis jedis = null;

        //noinspection Duplicates
        try {
            jedis = getJedisResource();
            return jedis.sort(key, dstkey);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("sort(" + key + ", " + dstkey + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long expire(String key, int seconds) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.expire(key, seconds);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.expire() error, key: {0}" + key, e);
            throw new RuntimeException("redis keys error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long expire(byte[] key, int seconds) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.expire(key, seconds);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.expire() error, key: " + Arrays.toString(key), e);
            throw new RuntimeException("redis keys error, key = " + Arrays.toString(key), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String get(String key) {
        String val;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            val = jedis.get(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.get() error, key: {0}" + key, e);
            throw new RuntimeException("redis get error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return val;
    }

    @Override
    public String getSet(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.getSet(key, value);
        } catch (Exception e) {
            if (jedis != null){
                jedis.close();
            }
            jedis = null;
            throw new RuntimeException("getset(" + key + "," + value + ")", e);
        } finally {
            if (jedis != null){
                jedis.close();
            }
        }
    }

    @Override
    public byte[] get(byte[] key) {
        byte[] val;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            val = jedis.get(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error(StringUtils.format("SimpleRedisDao.get() error, host:{0}, key: {1}", hostport, Arrays.toString(key)), e);
            throw new RuntimeException("redis get error, key = " + Arrays.toString(key), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return val;
    }

    @Override
    public Long hdel(String key, String field) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.hdel(key, field);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error(StringUtils.format("SimpleRedisDao.hdel() error, key: {0}, field", key, field), e);
            throw new RuntimeException("redis hdel error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String hget(String key, String field) {
        String value;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            value = jedis.hget(key, field);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.hget() error, key: " + key, e);
            throw new RuntimeException("redis hget error, key = " + key + " field = " + field, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return value;
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        Map<String, String> map;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            map = jedis.hgetAll(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.hgetAll() error, key: " + key, e);
            throw new RuntimeException("redis hgetAll error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return map;
    }

    @Override
    public Long hinc(String key, String field, long value) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.hincrBy(key, field, value);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.hinc() error, key: " + key, e);
            throw new RuntimeException("redis hinc error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Double hincrByFloat(byte[] key, byte[] field, double increment) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.hincrByFloat(key, field, increment);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("hincrByFloat(" + str(key) + ", " + str(field) + ", " + increment + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Set<String> hkeys(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.hkeys(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.hkeys() error, key: " + key, e);
            throw new RuntimeException("redis hkeys error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public long hlen(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.hlen(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.hlen() error, key: " + key, e);
            throw new RuntimeException("redis hlen error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.hmget(key, fields);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.hmget() error, key: " + key, e);
            throw new RuntimeException("redis hmget error, key = " + key + " fields = " + Arrays.toString(fields), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String hmset(String key, Map<String, String> map) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.hmset(key, map);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.hmset() error, key: " + key, e);
            throw new RuntimeException("redis hmset error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long hset(String key, String field, String value) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.hset(key, field, value);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.hset() error, key: " + key, e);
            throw new RuntimeException("redis hset error, key = " + key + " field = " + field + " value = " + value, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public boolean hsetnx(String key, String field, String value) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            Long hsetnx = jedis.hsetnx(key, field, value);
            return hsetnx != null && hsetnx > 0;
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error(this.getClass().getSimpleName() + ".hsetnx(" + key + ", " + field + ", " + value + ")", e);
            return false;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public List<byte[]> hvals(byte[] key) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.hvals(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("hvals(" + str(key) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public List<String> hvals(String key) {
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();
            return jedis.hvals(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("hvals(" + key + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public boolean hexists(String key, String field) {
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();
            Boolean hexists = jedis.hexists(key, field);
            return hexists != null && hexists;
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("hexists(" + key + ", " + field + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long inc(String key, long value) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.incrBy(key, value);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis inc error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Double incrByFloat(byte[] key, double increment) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.incrByFloat(key, increment);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("incrByFloat(" + str(key) + ", " + increment + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long append(byte[] key, byte[] val) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.append(key, val);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("append(" + str(key) + ", " + str(val) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long bitcount(byte[] key, long start, long end) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.bitcount(key, start, end);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("bitcount(" + str(key) + ", " + start + ", " + end + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.bitop(op, destKey, srcKeys);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("bitop(" + op + ", " + str(destKey) + ", " + str(srcKeys) + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long decr(String key, long val) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.decrBy(key, val);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis decr error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public byte[] lindex(byte[] key, int i) {
        byte[] result;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            result = jedis.lindex(key, i);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis lindex, error key = " + Arrays.toString(key) + " index = " + i, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return result;
    }

    @Override
    public Long linsert(byte[] key, BinaryClient.LIST_POSITION listPosition, byte[] pivot, byte[] value) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.linsert(key, listPosition, pivot, value);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("linsert(" + str(key) + ", " + listPosition + ", " + str(pivot) + ", " + str(value) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String lindex(String key, int i) {
        String result;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            result = jedis.lindex(key, i);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis lindex, error key = " + key + " index = " + i, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return result;
    }

    @Override
    public long llen(byte[] key) {
        Long length;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            length = jedis.llen(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis llen error, key = " + Arrays.toString(key), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return length != null ? length : 0L;
    }

    @Override
    public long llen(String key) {
        Long length;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            length = jedis.llen(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis llen error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return length != null ? length : 0L;
    }

    @Override
    public byte[] lpop(byte[] key) {
        byte[] result;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            result = jedis.lpop(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis lpop error, key = " + Arrays.toString(key), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return result;
    }

    @Override
    public String lpop(String key) {
        String result;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            result = jedis.lpop(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis lpop error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return result;
    }

    @Override
    public Long lpush(byte[] key, byte[] val) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.lpush(key, val);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis lpush error, key = " + Arrays.toString(key) + " value = " + Arrays.toString(val), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long lpushx(byte[] key, byte[] val) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.lpushx(key, val);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("lpushx(" + str(key) + ", " + str(val) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long lpush(String key, String val) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.lpush(key, val);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis lpush error, key = " + key + " value = " + val, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public List<String> lrange(String key, int start, int end) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.lrange(key, start, end);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis lrange error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long lrem(String key, long count, String value) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.lrem(key, count, value);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis lrem error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long lrem(byte[] key, int count, byte[] value) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.lrem(key, count, value);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("lrem(" + new String(key) + "," + count + "," + new String(value) + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    @Override
    public String lset(byte[] key, int index, byte[] value) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.lset(key, index, value);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("lset(" + str(key) + "," + index + "," + str(value) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String ltrim(String key, long start, long end) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.ltrim(key, start, end);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis ltrim error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public List<String> mget(String... keys) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.mget(keys);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis mget error, keys = " + Arrays.toString(keys), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String mset(String... keysvalues) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.mset(keysvalues);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis mset error, keys = " + Arrays.toString(keysvalues), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String psetex(byte[] key, int milliseconds, byte[] value) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.psetex(key, milliseconds, value);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("psetex(" + str(key) + ", " + milliseconds + ", " + str(value) + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public void pipeline(IExecutor<Pipeline> executor) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            Pipeline pipelined = jedis.pipelined();
            executor.execute(pipelined);
            pipelined.sync();
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis pipeline error", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public void pipeline(IExecutor<Pair<Pipeline, List<String>>> executor, List<String> keys) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            Pipeline pipelined = jedis.pipelined();
            executor.execute(Pair.of(pipelined, keys));
            pipelined.sync();
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis pipeline error", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long publish(String channel, String message) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.publish(channel, message);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis publish error, channel = " + channel, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public byte[] rpop(byte[] key) {
        byte[] result;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            result = jedis.rpop(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis rpop error, key = " + Arrays.toString(key), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return result;
    }

    @Override
    public String rpop(String key) {
        String result;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            result = jedis.rpop(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis rpop error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return result;
    }

    @Override
    public Long rpush(byte[] key, byte[] val) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.rpush(key, val);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis rpush error, key = " + Arrays.toString(key) + " value = " + Arrays.toString(val), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long rpushx(byte[] key, byte[] val) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.rpushx(key, val);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("rpushx(" + str(key) + ", " + str(val) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long rpush(String key, String val) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.rpush(key, val);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis rpush error, key = " + key + " value = " + val, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long sadd(String key, String val) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.sadd(key, val);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis sadd error, key = " + key + " value = " + val, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long sadd(String key, String[] members) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.sadd(key, members);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis.sadd(" + key + ", " + Arrays.toString(members) + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long scard(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.scard(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis sadd error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Set<byte[]> sdiff(byte[]... keys) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.sdiff(keys);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("sdiff(" + str(keys) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long sdiffstore(byte[] dstkey, byte[]... keys) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.sdiffstore(dstkey, keys);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("sdiffstore(" + str(dstkey) + "," + str(keys) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Set<byte[]> sinter(byte[]... keys) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.sinter(keys);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("sinter(" + str(keys) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long sinterstore(String destination, String... keys) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.sinterstore(destination, keys);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("sinterstore(" + destination + ", " + Arrays.toString(keys) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Set<byte[]> sunion(byte[]... keys) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.sunion(keys);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("sunion(" + str(keys) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long sunionstore(String destination, String... keys) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.sunionstore(destination, keys);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("sunionstore(" + destination + ", " + Arrays.toString(keys) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long zunionstore(byte[] dstkey, byte[]... sets) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.zunionstore(dstkey, sets);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("zunionstore(" + str(dstkey) + ", " + str(sets) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long zunionstore(String dstkey, String... sets) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.zunionstore(dstkey, sets);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("zunionstore(" + dstkey + ", " + Arrays.toString(sets) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public boolean setbit(String key, long offset, boolean value) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.setbit(key, offset, value);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis set error, key = " + key + " offset " + offset + " value = " + value, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String set(String key, String val) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.set(key, val);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis set error, key = " + key + " value = " + val, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String set(byte[] key, byte[] val) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.set(key, val);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis set error, key = " + new String(key) + " value = " + new String(val), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String set(String key, String val, int expireSecond) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.setex(key, expireSecond, val);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis setex error, key = " + key + " value = " + val, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }


    @Override
    public Long setnx(String key, String val) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.setnx(key, val);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis setnx error, key = " + key + " value = " + val, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long setrange(byte[] key, long offset, byte[] val) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.setrange(key, offset, val);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("setrange(" + str(key) + ", " + offset + ", " + str(val) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long strlen(byte[] key) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.strlen(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("strlen(" + str(key) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public boolean sismember(String key, String val) {
        boolean f;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            f = jedis.sismember(key, val);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis sismember error, key = " + key + " value = " + val, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return f;
    }

    @Override
    public void slaveofNoOne() {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            jedis.slaveofNoOne();
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
            }
            throw new RuntimeException("redis slaveofNoOne error", e);
        }
    }

    @Override
    public void slaveof(String mhost, int mport) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            jedis.slaveof(mhost, mport);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
            }
            throw new RuntimeException("redis slaveof error", e);
        }
    }

    @Override
    public Set<String> smembers(String key) {
        Set<String> set;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            set = jedis.smembers(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis smembers error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return set;
    }

    @Override
    public Long smove(byte[] srckey, byte[] dstkey, byte[] member) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.smove(srckey, dstkey, member);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("smove(" + str(srckey) + ", " + str(dstkey) + ", " + str(member) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String spop(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.spop(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis spop error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String srandmember(String key) {
        String value;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            value = jedis.srandmember(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis srandmember error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return value;
    }

    @Override
    public Long srem(String key, String val) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.srem(key, val);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis srem error, key = " + key + " value = " + val, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public void subscribe(final JedisPubSub jedisPubSub, final String... channel) {
        new Thread(() -> {
            while (!Thread.interrupted() && !isDestroyed()) {
                Jedis jedis = null;
                try {
                    jedis = getJedisResource();

                    jedisPubSubMap.put(jedisPubSub, channel);

                    jedis.subscribe(jedisPubSub, channel);
                } catch (Exception e) {
                    if (jedis != null) {
                        jedis.close();
                        jedis = null;
                    }
                    LOGGER.error("jedis.subscribe() error, channel: " + Arrays.toString(channel));

                    try {
                        Thread.sleep(RandomUtils.nextInt(SUBSCRIBE_RETRY_MAX_INTERVAL));
                    } catch (InterruptedException e1) {
                        LOGGER.error(e1);
                    }
                    continue;

                } finally {
                    if (jedis != null) {
                        jedis.close();
                    }
                }
                break;
            }
            jedisPubSubMap.remove(jedisPubSub);
            LOGGER.error("jedis.subscribe() finished: " + Arrays.toString(channel));
        }).start();
    }


    @Override
    public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
        while (true) {
            Jedis jedis = null;

            try {
                jedis = getJedisResource();
                jedis.psubscribe(jedisPubSub, patterns);
            } catch (Exception e) {
                if (jedis != null) {
                    jedis.close();
                    jedis = null;
                }
                LOGGER.error("psubscribe(" + jedisPubSub + "," + Arrays.toString(patterns) + ")", e);
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
            try {
                Thread.sleep(RandomUtils.nextInt(SUBSCRIBE_RETRY_MAX_INTERVAL));
            } catch (InterruptedException e) {
                LOGGER.error(e);
            }
        }
    }

    @Override
    public Long ttl(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.ttl(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }

            throw new RuntimeException("redis key error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String type(byte[] key) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.type(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("type(" + str(key) + ")");
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long zadd(String key, double score, String member) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zadd(key, score, member);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis zadd error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zadd(key, scoreMembers);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis.zadd(" + key + ", " + JsonUtils.toJSON(scoreMembers) + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long zadd(byte[] key, double score, byte[] member) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zadd(key, score, member);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            String errorStr = "redis zadd error, key=" + new String(key)
                    + ", score=" + score + ", member=" + new String(member);
            throw new RuntimeException(errorStr, e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
    }

    @Override
    public Long zcard(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zcard(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis zcard error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long zcard(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zcard(key);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            String errorStr = "zcard(" + new String(key) + ")";
            throw new RuntimeException(errorStr, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long zcount(String key, double min, double max) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zcount(key, min, max);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis zcount error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Double zincrby(String key, double increment, String member) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zincrby(key, increment, member);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis zincrby error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long zinterstore(byte[] dstkey, byte[]... sets) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.zinterstore(dstkey, sets);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("zinterstore(" + str(dstkey) + ", " + str(sets) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long zinterstore(byte[] dstkey, ZParams zParams, byte[]... sets) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.zinterstore(dstkey, zParams, sets);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("zinterstore(" + str(dstkey) + ", " + zParams + ", " + str(sets) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Set<String> zrange(String key, int start, int stop) {
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();
            return jedis.zrange(key, start, stop);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("zrange(" + key + ", " + start + ", " + stop + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * Use method 'zrange' instead.
     */
    @Override
    @Deprecated
    public Set<String> zrangeByOffset(String key, int offset, int count) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zrange(key, offset, offset + count);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis zrange error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * Use method 'zrangeWithScores' instead.
     */
    @Override
    @Deprecated
    public Set<Tuple> zrangeByOffsetWithScores(String key, int offset, int count) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zrangeWithScores(key, offset, offset + count);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis zrange error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Set<Tuple> zrangeByScore(String key, double min, double max) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zrangeByScoreWithScores(key, min, max);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis zrange error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Set<Tuple> zrangeByScore(String key, double min, double max, int offset, int count) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis zrange error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Set<Tuple> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis zrange error, key = " + Arrays.toString(key), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long zrank(String key, String member) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zrank(key, member);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis zrank error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long zrevrank(String key, String member) {

        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zrevrank(key, member);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("zrevrank(" + key + ", " + member + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long zrevrank(byte[] key, byte[] member) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zrevrank(key, member);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("zrevrank(" + new String(key) + ", " + new String(member) + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long zrem(String key, String member) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zrem(key, member);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis zrem error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis zremrangeByScore error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long zremrangebyrank(String key, long start, long stop) {
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();
            return jedis.zremrangeByRank(key, start, stop);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("zremrangeByRank(" + key + ", " + start + ", " + stop + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }


    @Override
    public Long zremrangebyrank(byte[] key, long start, long end) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.zremrangeByRank(key, start, end);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("zremrangebyrank(" + str(key) + ", " + start + ", " + end + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Set<String> zrevrangeV2(String key, int start, int end) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zrevrange(key, start, end);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis zrevrange error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * Use "zrevrangeWithScore" method instead
     **/
    @Deprecated
    @Override
    public Set<Tuple> zrevrangeByOffsetWithScores(String key, int offset, int count) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zrevrangeWithScores(key, offset, offset + count - 1);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis zrange error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Set<Tuple> zrevrangeWithScore(String key, int startIdx, int endIdx) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zrevrangeWithScores(key, startIdx, endIdx);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis zrevrange error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.zrevrangeByScore(key, max, min);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("zrevrangeByScore(" + key + ", " + max + ", " + min + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zrevrangeByScoreWithScores(key, max, min);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("redis zrevrangeByScore error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.zrevrangeByScore(key, max, min, offset, count);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("zrevrangeByScore(" + key + ", " + max + ", " + min + ", " + offset + ", " + count + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.zrevrangeByScoreWithScores() error, key: " + key);
            throw new RuntimeException("redis zrevrangeByScore error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Double zscore(String key, String member) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zscore(key, member);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.zscore() error, key: " + key, e);
            throw new RuntimeException("redis zscore error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Long zunionstore(byte[] dstkey, ZParams zParams, byte[]... sets) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.zunionstore(dstkey, zParams, sets);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("zunionstore(" + str(dstkey) + ", " + zParams + ", " + str(sets) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Object eval(byte[] script, byte[] keyCount, byte[][] params) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.eval(script, keyCount, params);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("eval(" + str(script) + ", " + str(keyCount) + ", " + str(params) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Object eval(String script, int keyCount, String... params) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.eval(script, keyCount, params);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("eval(" + script + ", " + keyCount + ", " + Arrays.toString(params) + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String scriptLoad(String script) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.scriptLoad(script);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            throw new RuntimeException("scriptLoad(" + script + ")", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Object evalsha(String script, int keyCount, String... params) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return jedis.evalsha(script, keyCount, params);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("eval(" + script + ", " + keyCount + ", " + Arrays.toString(params) + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SimpleRedisDao other = (SimpleRedisDao) obj;
        if (hostport == null) {
            return other.hostport == null;
        } else return hostport.equals(other.hostport);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((hostport == null) ? 0 : hostport.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return "SimpleRedisDao [hostport=" + hostport + "]";
    }


    private Jedis getJedisResource() {
        long start = System.currentTimeMillis();
        try {
            return getJedisPool().getResource();
        } catch (Exception e) {
            LOGGER.error(StringUtils.format("getJedisResource error, timeCost={0}, hostport = {1}", System.currentTimeMillis() - start, hostport.toString()), e);

            // ping()
            try {
                hostport.ping();
            } catch (Throwable t) {
                throw new RuntimeException("hostport.ping() fail: " + hostport.toString() + ", " + t.getMessage());
            }

            // pool is full
            if (e instanceof JedisConnectionException
                    && e.getCause() instanceof NoSuchElementException) {
                throw new RuntimeException("jedis pool is full: " + hostport.toString(), e);
            }

            // no reconnect
            throw new RuntimeException("getJedisResource() error: " + hostport.toString(), e);

        } finally {
            long end = System.currentTimeMillis();
            LOGGER.info("getJedisResource()" + (end - start) + " " + hostport);
//            RedisDaoLog.info("getJedisPool().getResource()", "getJedisResource()", (end - start), hostport);
        }
    }


    @Override
    public boolean getbit(String key, long offset) {

        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.getbit(key, offset);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.bitget() error, key: " + key, e);
            throw new RuntimeException("redis get error, key = " + key + " offset " + offset);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String getrange(byte[] key, long startOffset, long endOffset) {
        Jedis jedis = null;

        try {
            jedis = getJedisResource();
            return safeEncode(jedis.getrange(key, startOffset, endOffset));
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("getrange(" + str(key) + ", " + startOffset + ", " + endOffset + ")", e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public List<Object> multi(String key, IExecutor<Transaction> executor) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();

            jedis.watch(key);

            Transaction transaction = jedis.multi();
            executor.execute(transaction);
            return transaction.exec();
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error(this.getClass().getSimpleName() + ".multi(" + key + ", " + executor + ")", e);
            return null;
        } finally {
            if (jedis != null) {
                jedis.unwatch();
                jedis.close();
            }
        }
    }

    @Override
    public Map<String, String> info() {
        Map<String, String> info;
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            String val = jedis.info();
            info = paresInfo(val);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.info() error", e);
            throw new RuntimeException("redis info error", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return info;
    }

    private Map<String, String> paresInfo(String val) {
        Map<String, String> info = new HashMap<>();
        String[] lines = val.split("\r\n");
        for (String line : lines) {
            String[] kv = line.split(":");
            if (kv.length > 1) {
                info.put(kv[0], kv[1]);
            }
        }
        return info;
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, int start, int end) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.zrangeWithScores(key, start, end);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.zrangeWithScores() error, key: " + key, e);
            throw new RuntimeException("redis zrangeWithScores error, key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public List<Slowlog> slowlogGet() {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.slowlogGet();
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.slowlogGet() error", e);
            throw new RuntimeException("redis slowlogGet error", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public List<Slowlog> slowlogGet(long entries) {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.slowlogGet(entries);
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.slowlogGet() error, entries: " + entries, e);
            throw new RuntimeException("redis slowlogGet error, entries = " + entries, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public long slowlogLen() {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return jedis.slowlogLen();
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.slowlogLen() error", e);
            throw new RuntimeException("redis slowlogLen error", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public byte[] slowlogReset() {
        Jedis jedis = null;
        try {
            jedis = getJedisResource();
            return safeEncode(jedis.slowlogReset());
        } catch (Exception e) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
            }
            LOGGER.error("SimpleRedisDao.slowlogReset() error", e);
            throw new RuntimeException("redis slowlogReset error", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public Map<JedisPubSub, String[]> getJedisPubsubMap() {
        return jedisPubSubMap;
    }

    private String str(byte[] bytes) {
        return bytes == null ? "null" : new String(bytes, Charsets.UTF_8);
    }

    private String str(byte[][] keys) {
        return Joiner.on(",").join(Iterators.transform(Iterators.forArray(keys), this::str));
    }

    private String safeEncode(byte[] bytes) {
        return bytes == null ? null : SafeEncoder.encode(bytes);
    }

    private byte[] safeEncode(String str) {
        return str == null ? null : SafeEncoder.encode(str);
    }
}
