package com.redis.store.monitor;

import com.redis.store.dao.IRedisDao;
import com.redis.store.service.NamedThreadFactory;
import com.redis.store.utils.LogCreator;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Author LTY
 * Date 2018/05/24
 */
public class JedisPoolConnMonitorTask implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(JedisPoolConnMonitorTask.class);

    private static final String HEADLINE = String.format("%-13s  %-13s  %-13s  %-13s  %-30s", "CONN_ACTIVE", "CONN_IDLE", "THREAD_WAIT", "MEAN_WAIT_MS", "REDIS_HOST_PORT");

    private static final Logger JEDIS_POOL_CONN_MONITOR_LOGGER = LogCreator.createLogger("JedisPoolConnMonitor", System.getProperty("redisLogPath", "worker"), new PatternLayout("%m%n"), false);

    private Map<String, IRedisDao> redisDaoMap;

    public JedisPoolConnMonitorTask(Map<String, IRedisDao> redisDaoMap) {
        this.redisDaoMap = redisDaoMap;
    }


    public void start() {
        Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("JedisPoolConnMonitorTask"))
                .scheduleAtFixedRate(this, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        try {
            if (redisDaoMap == null || redisDaoMap.size() < 1) {
                return;
            }
            JEDIS_POOL_CONN_MONITOR_LOGGER.info(DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss"));
            JEDIS_POOL_CONN_MONITOR_LOGGER.info(HEADLINE);

            List<IRedisDao> redisDaos = new ArrayList<>(redisDaoMap.values());
            redisDaos.sort(Comparator.comparing(IRedisDao::getHostPort));

            List<Pair<Integer, String>> logs = new ArrayList<>(redisDaos.size());
            for (IRedisDao redisDao : redisDaos) {
                JedisPool jedisPool = redisDao.getJedisPool();
                int connActive = jedisPool.getNumActive();
                int connIdle = jedisPool.getNumIdle();
                int threadWait = jedisPool.getNumWaiters();
                long meanWaitTimeMillis = jedisPool.getMeanBorrowWaitTimeMillis();
                String redisHostPort = redisDao.getHostPort();
                logs.add(Pair.of(threadWait, String.format("%-13s  %-13s  %-13s  %-13s  %-30s",
                        connActive, connIdle, threadWait, meanWaitTimeMillis, redisHostPort)));
            }
            logs.sort((o1, o2) -> o2.getLeft().compareTo(o1.getLeft()));
            int maxLine = Math.min(logs.size(), 10);
            for (int i = 0; i < maxLine; i++) {
                JEDIS_POOL_CONN_MONITOR_LOGGER.info(logs.get(i).getRight());
            }
        } catch (Exception e) {
            LOGGER.error("jedisPoolConnMonitor error", e);
        }
    }
}
