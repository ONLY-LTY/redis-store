package com.redis.store.dao;

import com.redis.store.executor.IExecutor;
import org.apache.commons.lang3.tuple.Pair;
import redis.clients.jedis.*;
import redis.clients.util.Slowlog;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IRedisDao {
    String getHostPort();

    JedisPool getJedisPool();

    Set<String> keys(String var1);

    Long persist(byte[] var1);

    Long pexpire(byte[] var1, int var2);

    Long pexpireAt(byte[] var1, long var2);

    Long pttl(byte[] var1);

    void destroy();

    boolean isDestroyed();

    byte[] brpoplpush(byte[] var1, byte[] var2, int var3);

    byte[] rpoplpush(byte[] var1, byte[] var2);

    List<String> blpop(String var1, int var2);

    List<String> blpop(int var1, String... var2);

    Long del(String var1);

    Long del(String... var1);

    byte[] dump(byte[] var1);

    boolean exists(String var1);

    String rename(String var1, String var2);

    Long renamenx(String var1, String var2);

    String restore(byte[] var1, int var2, byte[] var3);

    List<String> sort(String var1);

    List<String> sort(String var1, SortingParams var2);

    Long sort(String var1, SortingParams var2, String var3);

    Long sort(String var1, String var2);

    Long expire(String var1, int var2);

    Long expire(byte[] var1, int var2);

    String get(String var1);

    String getSet(String var1, String var2);

    byte[] get(byte[] var1);

    Long hdel(String var1, String var2);

    String hget(String var1, String var2);

    Map<String, String> hgetAll(String var1);

    Long hinc(String var1, String var2, long var3);

    Double hincrByFloat(byte[] var1, byte[] var2, double var3);

    Set<String> hkeys(String var1);

    long hlen(String var1);

    List<String> hmget(String var1, String... var2);

    String hmset(String var1, Map<String, String> var2);

    Long hset(String var1, String var2, String var3);

    boolean hsetnx(String var1, String var2, String var3);

    List<byte[]> hvals(byte[] var1);

    List<String> hvals(String var1);

    boolean hexists(String var1, String var2);

    Long inc(String var1, long var2);

    Double incrByFloat(byte[] var1, double var2);

    Long append(byte[] var1, byte[] var2);

    Long bitcount(byte[] var1, long var2, long var4);

    Long bitop(BitOP var1, byte[] var2, byte[]... var3);

    Long decr(String var1, long var2);

    byte[] lindex(byte[] var1, int var2);

    String lindex(String var1, int var2);

    Long linsert(byte[] key, BinaryClient.LIST_POSITION listPosition, byte[] pivot, byte[] value);

    long llen(byte[] var1);

    long llen(String var1);

    byte[] lpop(byte[] var1);

    String lpop(String var1);

    Long lpush(byte[] var1, byte[] var2);

    Long lpushx(byte[] var1, byte[] var2);

    Long lpush(String var1, String var2);

    List<String> lrange(String var1, int var2, int var3);

    Long lrem(String var1, long var2, String var4);

    Long lrem(byte[] var1, int var2, byte[] var3);

    String lset(byte[] var1, int var2, byte[] var3);

    String ltrim(String var1, long var2, long var4);

    List<String> mget(String... var1);

    String mset(String... var1);

    String psetex(byte[] var1, int var2, byte[] var3);

    void pipeline(IExecutor<Pipeline> var1);

    void pipeline(IExecutor<Pair<Pipeline, List<String>>> var1, List<String> var2);

    Long publish(String var1, String var2);

    byte[] rpop(byte[] var1);

    String rpop(String var1);

    Long rpush(byte[] var1, byte[] var2);

    Long rpushx(byte[] var1, byte[] var2);

    Long rpush(String var1, String var2);

    Long sadd(String var1, String var2);

    Long sadd(String var1, String[] var2);

    Long scard(String var1);

    String set(String var1, String var2);

    String set(byte[] var1, byte[] var2);

    String set(String var1, String var2, int var3);

    Long setnx(String var1, String var2);

    Long setrange(byte[] var1, long var2, byte[] var4);

    Long strlen(byte[] var1);

    boolean sismember(String var1, String var2);

    Set<String> smembers(String var1);

    Long smove(byte[] var1, byte[] var2, byte[] var3);

    String spop(String var1);

    String srandmember(String var1);

    Long srem(String var1, String var2);


    void subscribe(JedisPubSub var1, String... var2);

    void psubscribe(JedisPubSub var1, String... var2);

    Long ttl(String var1);

    String type(byte[] var1);

    Long zadd(String var1, double var2, String var4);

    Long zadd(String var1, Map<String,Double> var2);

    Long zadd(byte[] var1, double var2, byte[] var4);

    Long zcard(String var1);

    Long zcard(byte[] var1);

    Long zcount(String var1, double var2, double var4);

    Double zincrby(String var1, double var2, String var4);

    Long zinterstore(byte[] var1, byte[]... var2);

    Long zinterstore(byte[] var1, ZParams var2, byte[]... var3);

    Set<String> zrange(String var1, int var2, int var3);

    /** @deprecated */
    @Deprecated
    Set<String> zrangeByOffset(String var1, int var2, int var3);

    /** @deprecated */
    @Deprecated
    Set<Tuple> zrangeByOffsetWithScores(String var1, int var2, int var3);

    Set<Tuple> zrangeByScore(String var1, double var2, double var4);

    Set<Tuple> zrangeByScore(String var1, double var2, double var4, int var6, int var7);

    Set<Tuple> zrangeByScore(byte[] var1, double var2, double var4, int var6, int var7);

    Long zrank(String var1, String var2);

    Long zrevrank(String var1, String var2);

    Long zrevrank(byte[] var1, byte[] var2);

    Long zrem(String var1, String var2);

    Long zremrangeByScore(String var1, double var2, double var4);

    Long zremrangebyrank(String var1, long var2, long var4);

    Long zremrangebyrank(byte[] var1, long var2, long var4);

    Set<String> zrevrangeV2(String var1, int var2, int var3);

    Set<Tuple> zrevrangeByOffsetWithScores(String var1, int var2, int var3);

    Set<Tuple> zrevrangeWithScore(String var1, int var2, int var3);

    Set<String> zrevrangeByScore(String var1, double var2, double var4);

    Set<Tuple> zrevrangeByScoreWithScores(String var1, double var2, double var4);

    Set<String> zrevrangeByScore(String var1, double var2, double var4, int var6, int var7);

    Set<Tuple> zrevrangeByScoreWithScores(String var1, double var2, double var4, int var6, int var7);

    Double zscore(String var1, String var2);

    void slaveofNoOne();

    void slaveof(String var1, int var2);

    Set<byte[]> sdiff(byte[]... var1);

    Long sdiffstore(byte[] var1, byte[]... var2);

    Set<byte[]> sinter(byte[]... var1);

    Long sinterstore(String var1, String... var2);

    Set<byte[]> sunion(byte[]... var1);

    Long sunionstore(String var1, String... var2);

    Long zunionstore(byte[] var1, byte[]... var2);

    Long zunionstore(String var1, String... var2);

    boolean setbit(String var1, long var2, boolean var4);

    Long zunionstore(byte[] var1, ZParams var2, byte[]... var3);

    Object eval(byte[] var1, byte[] var2, byte[][] var3);

    Object eval(String var1, int var2, String... var3);

    String scriptLoad(String var1);

    Object evalsha(String var1, int var2, String... var3);

    boolean getbit(String var1, long var2);

    String getrange(byte[] var1, long var2, long var4);

    List<Object> multi(String var1, IExecutor<Transaction> var2);

    Map<String, String> info();

    Set<Tuple> zrangeWithScores(String var1, int var2, int var3);

    List<Slowlog> slowlogGet();

    List<Slowlog> slowlogGet(long var1);

    long slowlogLen();

    byte[] slowlogReset();

    Map<JedisPubSub, String[]> getJedisPubsubMap();

    boolean ping();
}
