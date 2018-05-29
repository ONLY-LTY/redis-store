package com.redis.store.proxy;

import com.redis.store.constants.Constants;
import com.redis.store.dao.IRedisDao;
import com.redis.store.util.JsonUtils;
import org.apache.log4j.Logger;

import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class FailFast {

    private static final Logger LOGGER = Logger.getLogger(FailFast.class);

    private static final boolean FAIL_FAST_SWITCH = System.getProperty(Constants.REDIS_CONFIG_FAIL_FAST_SWITCH_KEY, "off").equals("on");
    private static final long CHECK_FAIL_TASK_DELAY = getSystemIntProperty("CHECK_FAIL_TASK_DELAY") != 0 ? getSystemIntProperty("CHECK_FAIL_TASK_DELAY") : 3; // seconds
    private static final long CHECK_FAIL_TASK_PERIOD = getSystemIntProperty("CHECK_FAIL_TASK_PERIOD") != 0 ? getSystemIntProperty("CHECK_FAIL_TASK_PERIOD") : 3;
    private static final long CHECK_SUCCEED_TASK_DELAY = getSystemIntProperty("CHECK_SUCCEED_TASK_DELAY") != 0 ? getSystemIntProperty("CHECK_SUCCEED_TASK_DELAY") : 1;
    private static final long CHECK_SUCCEED_TASK_PERIOD = getSystemIntProperty("CHECK_SUCCEED_TASK_PERIOD") != 0 ? getSystemIntProperty("CHECK_SUCCEED_TASK_PERIOD") : 1;
    private static final long FAIL_FAST_COUNT_THRESHOLD = getSystemIntProperty("FAIL_FAST_COUNT_THRESHOLD") != 0 ? getSystemIntProperty("FAIL_FAST_COUNT_THRESHOLD") : 10;
    private static final double FAIL_FAST_RATIO_THRESHOLD = getSystemDoubleProperty("FAIL_FAST_RATIO_THRESHOLD") != 0 ? getSystemDoubleProperty("FAIL_FAST_RATIO_THRESHOLD") : 0.9;

    private static ConcurrentHashMap<String, Counter> failFastCounterMap = new ConcurrentHashMap<>();
    private static CopyOnWriteArrayList<String> failList = new CopyOnWriteArrayList<>();

    static {
        LOGGER.info("the failFast param is:[" +
                " CHECK_FAIL_TASK_DELAY:" + CHECK_FAIL_TASK_DELAY + "," +
                " CHECK_FAIL_TASK_PERIOD:" + CHECK_FAIL_TASK_PERIOD + "," +
                " CHECK_SUCCEED_TASK_DELAY" + CHECK_SUCCEED_TASK_DELAY + "," +
                " CHECK_SUCCEED_TASK_PERIOD:" + CHECK_SUCCEED_TASK_PERIOD + "," +
                " FAIL_FAST_COUNT_THRESHOLD" + FAIL_FAST_COUNT_THRESHOLD + "," +
                " FAIL_FAST_RATIO_THRESHOLD:" + FAIL_FAST_RATIO_THRESHOLD);
        if (FAIL_FAST_SWITCH) {
            start();
        }
    }

    public static boolean isInFailFast(String hostPort) {
        return failList.contains(hostPort);
    }

    private static int getSystemIntProperty(String key) {
        if (System.getProperty(key) != null) {
            try {
                return Integer.parseInt(System.getProperty(key));
            } catch (Exception e) {
                return 0;
            }
        }
        return 0;
    }

    private static double getSystemDoubleProperty(String key) {
        if (System.getProperty(key) != null) {
            try {
                return Double.parseDouble(System.getProperty(key));
            } catch (Exception e) {
                return 0;
            }
        }
        return 0;
    }

    public static boolean getFailFastSwitch() {
        return FAIL_FAST_SWITCH;
    }

    public static void succeed(String key) {
        Counter succeedCounter = getFailFastCounter(key);
        succeedCounter.succeed.incrementAndGet();
    }

    public static void fail(String key) {
        Counter failedCounter = getFailFastCounter(key);
        failedCounter.fail.incrementAndGet();
    }

    private static Counter getFailFastCounter(String key) {
        Counter counter = failFastCounterMap.get(key);
        if (counter == null) {
            counter = new Counter();
            Counter tmp = failFastCounterMap.putIfAbsent(key, counter);
            if (tmp != null) {
                counter = tmp;
            }
        }
        return counter;
    }

    public static boolean isFailFast(String hostPort) {
        return failList.contains(hostPort);
    }

    public static void start() {
        ScheduledExecutorService es = Executors.newScheduledThreadPool(2);
        es.scheduleAtFixedRate(new CheckFailTask(failFastCounterMap, failList), CHECK_FAIL_TASK_DELAY, CHECK_FAIL_TASK_PERIOD, TimeUnit.SECONDS);
        es.scheduleAtFixedRate(new CheckSucceedTask(failList), CHECK_SUCCEED_TASK_DELAY, CHECK_SUCCEED_TASK_PERIOD, TimeUnit.SECONDS);
    }

    static class CheckFailTask implements Runnable {
        private ConcurrentMap<String, Counter> fastFailCounterMap;
        private CopyOnWriteArrayList<String> failList;

        public CheckFailTask(ConcurrentMap<String, Counter> fastFailCounterMap, CopyOnWriteArrayList<String> failList) {
            this.fastFailCounterMap = fastFailCounterMap;
            this.failList = failList;
        }

        @Override
        public void run() {
            for (Entry<String, Counter> entry : fastFailCounterMap.entrySet()) {
                Long fail = entry.getValue().fail.get();
                Long succeed = entry.getValue().succeed.get();

                if (fail != 0) {
                    LOGGER.info(String.format("failFast counter, hostPort: %s  succeed: %s fail: %s", entry.getKey(), succeed.toString(), fail));
                }

                if (fail >= FAIL_FAST_COUNT_THRESHOLD && ((double) fail / (double) (fail + succeed) > FAIL_FAST_RATIO_THRESHOLD)) {
                    LOGGER.error("add to failFast list fail: hostPort " + entry.getKey());
                    failList.addIfAbsent(entry.getKey());
                }
                entry.getValue().succeed.set(0);
                entry.getValue().fail.set(0);
            }
        }
    }

    static class CheckSucceedTask implements Runnable {
        private CopyOnWriteArrayList<String> failList;

        public CheckSucceedTask(CopyOnWriteArrayList<String> failList) {
            this.failList = failList;
        }

        @Override
        public void run() {
            for (String hostPort : failList) {
                IRedisDao redisDao = RedisConfigStoreProxy.getByHostPort(hostPort);
                if (redisDao != null) {
                    try {
                        boolean ping = redisDao.ping();
                        if (ping) {
                            LOGGER.error(String.format("fail fast succeed: hostPort:%s", hostPort));
                            failList.remove(hostPort);
                        }
                    } catch (Exception e) {
                        LOGGER.error("failFast check fail:" + hostPort, e);
                    }
                } else {
                    failList.remove(hostPort);
                }
            }

            if (!failList.isEmpty()) {
                LOGGER.warn("the fail fast list is not empty:" + JsonUtils.toJSON(failList));
            }
        }
    }

    static class Counter {
        AtomicLong succeed = new AtomicLong(0);
        AtomicLong fail = new AtomicLong(0);
    }
}
