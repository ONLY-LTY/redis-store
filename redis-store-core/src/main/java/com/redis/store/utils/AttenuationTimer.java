package com.redis.store.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *  衰减器：用于在时间上逐步减小某个开关的触发条件。
 */
public class AttenuationTimer <T> {

    private static final int MIN_INTERVAL_SECOND_DEFAULT = 1;
    private static final int MAX_INTERVAL_SECOND_DEFAULT = 60 * 60 * 24; // 1天

    private final long minInterval;
    private final long maxInterval;

    private ConcurrentMap<T, AtomicLong> records = new ConcurrentHashMap<T, AtomicLong>(1000);
    private ConcurrentMap<T, AtomicLong> intervals = new ConcurrentHashMap<T, AtomicLong>(1000);

    public AttenuationTimer() {
        this(MIN_INTERVAL_SECOND_DEFAULT, MAX_INTERVAL_SECOND_DEFAULT);
    }

    /**
     * @param minIntervalSecond
     *            　最小间隔时间，单位秒
     * @param maxIntervalSecond
     *            　最大间隔时间，单位秒
     */
    public AttenuationTimer(int minIntervalSecond, int maxIntervalSecond) {
        this.minInterval = minIntervalSecond * 1000L;
        this.maxInterval = maxIntervalSecond * 1000L;
    }

    /**
     * 是否到达调度时间。
     */
    public boolean isOnTime(T target) {
        if (!records.containsKey(target)) {
            records.putIfAbsent(target, new AtomicLong(0));
        }
        if (!intervals.containsKey(target)) {
            intervals.putIfAbsent(target, new AtomicLong(minInterval));
        }

        AtomicLong last = records.get(target);
        long now = System.currentTimeMillis();
        if (last.get() == 0L) {
            // 首次设置时间
            if (last.compareAndSet(0L, now)) {
                return true;
            }
        }

        AtomicLong interval = intervals.get(target);
        long currentInterval = interval.get();
        if ((now - last.get()) >= currentInterval) {
            // 到达调度时间
            long nextInterval = currentInterval * 2 < maxInterval ? currentInterval * 2 : maxInterval;
            if (interval.compareAndSet(currentInterval, nextInterval)) {
                // 设置下次时间间隔成功，命中
                last.set(now);
                return true;
            }
        }

        return false;
    }

    /**
     * 重置时间记录器和时间间隔。
     */
    public void reset(T target) {
        if (!records.containsKey(target)) {
            records.putIfAbsent(target, new AtomicLong(0));
        }

        if (!intervals.containsKey(target)) {
            intervals.putIfAbsent(target, new AtomicLong(minInterval));
        } else {
            intervals.get(target).set(minInterval);
        }
    }

    /**
     * 移除目标。
     */
    public void remove(T target) {
        records.remove(target);
        intervals.remove(target);
    }

}
