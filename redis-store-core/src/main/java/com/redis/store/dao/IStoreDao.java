package com.redis.store.dao;

import com.redis.store.executor.IExecutor;
import org.apache.commons.lang3.tuple.Pair;
import redis.clients.jedis.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IStoreDao {

    List<String> blpop(String hashKey, String key, int timeout);

    List<String> blpop(String hashKey, int timeout, String... keys);

    Long del(String hashKey, String key);

    byte[] dump(String hashKey, byte[] key);

    boolean exists(String hashKey, String key);

    String rename(String hashKey, String key, String newKey);

    Long renamenx(String hashKey, String key, String newKey);

    String restore(String hashKey, byte[] key, int ttl, byte[] serializedValue);

    Long expire(String hashKey, String key, int seconds);

    Long expire(String hashKey, byte[] key, int seconds);

    String get(String hashKey, String key);

    byte[] get(String hashKey, byte[] key);

    Long hdel(String hashKey, String key, String fields);

    String hget(String hashKey, String key, String field);

    boolean hexists(String hashKey, String key, String field);

    Map<String, String> hgetAll(String hashKey, String key);

    Long hinc(String hashKey, String key, String field, long val);

    Double hincrByFloat(String hashKey, byte[] key, byte[] field, double increment);

    Set<String> hkeys(String hashKey, String key);

    long hlen(String hashKey, String key);

    List<String> hmget(String hashKey, String key, String... fields);

    String hmset(String hashKey, String key, Map<String, String> map);

    Long hset(String hashKey, String key, String field, String val);

    Long inc(String hashKey, String key, long val);

    Double incrByFloat(String hashKey, byte[] key, double increment);

    Long decr(String hashKey, String key, long val);

    byte[] lindex(String hashKey, byte[] key, int i);

    String lindex(String hashKey, String key, int i);

    long llen(String hashKey, byte[] key);

    long llen(String hashKey, String key);

    byte[] lpop(String hashKey, byte[] key);

    String lpop(String hashKey, String key);

    Long lpush(String hashKey, byte[] key, byte[] val);

    Long lpush(String hashKey, String key, String val);

    List<String> lrange(String hashKey, String key, int start, int end);

    Long lrem(String hashKey, String key, long count, String value);

    String ltrim(String hashKey, String key, long start, long end);

    List<String> mget(String hashKey, String... keys);

    String mset(String hashKey, String... keysvalues);

    String psetex(String hashKey, byte[] key, int milliseconds, byte[] value);

    Long publish(String hashKey, String channel, String message);

    byte[] rpop(String hashKey, byte[] key);

    String rpop(String hashKey, String key);

    Long rpush(String hashKey, byte[] key, byte[] val);

    Long rpush(String hashKey, String key, String val);

    Long sadd(String hashKey, String key, String val);

    Long sadd(String hashKey, String key, String[] members);

    Long scard(String hashKey, String key);

    String set(String hashKey, String key, String val);

    String set(String hashKey, byte[] key, byte[] val);

    String set(String hashKey, String key, String val, int expireSecond);

    Long setnx(String hashKey, String key, String val);

    boolean sismember(String hashKey, String key, String val);

    Set<String> smembers(String hashKey, String key);

    String spop(String hashKey, String key);

    String srandmember(String hashKey, String key);

    Long srem(String hashKey, String key, String val);

    void pipeline(String hashKey, IExecutor<Pipeline> executor);

    void pipeline(String hashKey, IExecutor<Pair<Pipeline, List<String>>> executor, List<String> keys);

    void subscribe(String hashKey, JedisPubSub jedisPubSub, String... channel);

    Long ttl(String hashKey, String key);

    Long zadd(String hashKey, String key, double score, String member);

    Long zadd(String hashKey, String key, Map<String, Double> scoreMembers);

    Long zcard(String hashKey, String key);

    Long zcount(String hashKey, String key, double min, double max);

    Double zincrby(String hashKey, String key, double increment, String member);

    Set<String> zrange(String hashkey, String key, int start, int stop);

    /**
     * Use method 'zrange' instead.
     */
    @Deprecated
    Set<String> zrangeByOffset(String hashKey, String key, int offset, int count);

    @Deprecated
    Set<Tuple> zrangeByOffsetWithScores(String hashKey, String key, int offset,
                                        int count);

    Set<Tuple> zrangeWithScores(String hashKey, String key, int start, int end);

    Set<Tuple> zrangeByScore(String hashKey, String key, double min, double max);

    Set<Tuple> zrangeByScore(String hashKey, String key, double min,
                             double max, int offset, int count);

    Set<Tuple> zrangeByScore(String hashKey, byte[] key, double min,
                             double max, int offset, int count);

    Long zrank(String hashKey, String key, String member);

    Long zrem(String hashKey, String key, String member);

    Long zremrangeByScore(String hashKey, String key, double start, double end);

    Long zremrangebyrank(String hashkey, String key, long start, long stop);

    @Deprecated
    Long zremrangeByRank(String hashKey, String key, int offset, int count);

    Long zremrangebyrank(String hashKey, byte[] key, long start, long end);

    @Deprecated
    Set<String> zrevrange(String hashKey, String key, int offset, int count);

    Set<String> zrevrangeV2(String hashKey, String key, int start, int end);

    Set<Tuple> zrevrangeByOffsetWithScores(String hashKey, String key,
                                           int offset, int count);

    Set<Tuple> zrevrangeWithScore(String hashKey, String key, int startIdx,
                                  int endIdx);

    Set<String> zrevrangeByScore(String hashkey, String key, double max, double min);

    Set<Tuple> zrevrangeByScoreWithScores(String hashKey, String key, double max, double min);

    Set<String> zrevrangeByScore(String hashkey, String key, double max, double min, int offset, int count);

    Set<Tuple> zrevrangeByScoreWithScores(String hashKey, String key, double max, double min, int offset, int count);

    Double zscore(String hashKey, String key, String member);

    boolean setbit(String hashKey, String key, long offset, boolean value);

    boolean getbit(String hashKey, String key, long offset);

    String getSet(String hashKey, String key, String val);

    Long persist(String hashKey, byte[] key);

    Long pexpire(String hashKey, byte[] key, int milliseconds);

    Long pexpireAt(String hashKey, byte[] key, long millisecondsTimestamp);

    Long pttl(String hashKey, byte[] key);

    List<byte[]> hvals(String hashKey, byte[] key);

    List<String> hvals(String hashKey, String key);

    Long append(String hashKey, byte[] key, byte[] val);

    Long bitcount(String hashKey, byte[] key, long start, long end);

    Long bitop(String hashKey, BitOP op, byte[] destKey, byte[]... srcKeys);

    Long linsert(String hashKey, byte[] key, BinaryClient.LIST_POSITION listPosition, byte[] pivot, byte[] value);

    Long lpushx(String hashKey, byte[] key, byte[] val);

    String lset(String hashKey, byte[] key, int index, byte[] value);

    Long rpushx(String hashKey, byte[] key, byte[] val);

    Long setrange(String hashKey, byte[] key, long offset, byte[] val);

    Long strlen(String hashKey, byte[] key);

    Long smove(String hashKey, byte[] srckey, byte[] dstkey, byte[] member);

    String type(String hashKey, byte[] key);

    Long zinterstore(String hashKey, byte[] dstkey, byte[]... sets);

    Long zinterstore(String hashKey, byte[] dstkey, ZParams zParams, byte[]... sets);

    Set<byte[]> sdiff(String hashKey, byte[]... keys);

    Long sdiffstore(String hashKey, byte[] dstkey, byte[]... keys);

    Set<byte[]> sinter(String hashKey, byte[]... keys);

    Set<byte[]> sunion(String hashKey, byte[]... keys);

    Long zunionstore(String hashKey, byte[] dstkey, byte[]... sets);

    Long zunionstore(String hashKey, byte[] dstkey, ZParams zParams, byte[]... sets);

    Object eval(String hashKey, byte[] script, byte[] keyCount, byte[][] params);

    Object eval(String script, int keyCount, String... params);

    String scriptLoad(String hashkey, String script);

    Object evalsha(String hashKey, String script, int keyCount, String... params);

    String getrange(String hashKey, byte[] key, long startOffset, long endOffset);

    byte[] rpoplpush(String hashKey, byte[] key, byte[] dest);

    boolean hsetnx(String hashKey, String key, String field, String value);

    Long sinterstore(String hashKey, String destination, String... keys);

    Long sunionstore(String hashKey, String destination, String... keys);

    Long zrevrank(String hashKey, byte[] key, byte[] member);

    List<String> sort(String hashkey, String key);

    List<String> sort(String hashkey, String key, SortingParams sortingParams);

    Long sort(String hashkey, String key, SortingParams sortingParams, String dstkey);

    Long sort(String hashkey, String key, String dstkey);
}
