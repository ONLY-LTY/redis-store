package com.redis.store.proxy.ha;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yangzhongjie
 *         Created:2016-11-28
 */
public class DefaultIsolatorStrategy extends IsolatorStrategy {

    private final long defaultDelayTime = 1000 * 60 * 5L; //10min
    //清空count时间
    private final long INTERVAL = 1000 * 60 * 1L;  //1min
    //处罚时间
    private final long PUNISH_TIME = 1000 * 60 * 4L; //4min
    //规定时间内最大隔离个数
    private final int DEFAULT_MAX_ISOLATOR_SIZE = 2;
    //处罚隔离计数器
    private final AtomicInteger punishCounter = new AtomicInteger(0);
    //current control lock
    private final Lock isolatorLock = new ReentrantLock();
    //清空count计数时间
    private long beginTime;
    //下次释放结束时间
    private long punishTimeEnd;

    @Override
    void register(Isolator isolator) {
        isolatorLock.lock();
        try {
            if (isRegistered()) {
                super.register(isolator);
            } else {
                LOG.error("Register too fast , ignore this : " + isolator.getHostPort());
            }
        } finally {
            isolatorLock.unlock();
        }
    }

    @Override
    boolean processor(Isolator isolator) {
        final long isolateTime = isolator.getTime();
        boolean result = isolateTime + defaultDelayTime <= System.currentTimeMillis();
        if (!result) {
            LOG.error("Isolator not released name : " + isolator.getHostPort());
        }
        return result;
    }

    private boolean isRegistered() {
        if ((punishTimeEnd > 0) && (punishTimeEnd > System.currentTimeMillis())) {
            return false;
        }
        if (punishCounter.getAndIncrement() == 0) {
            beginTime = System.currentTimeMillis();
        } else {
            if (punishCounter.get() > DEFAULT_MAX_ISOLATOR_SIZE) {
                punishCounter.set(0);
                punishTimeEnd = PUNISH_TIME + System.currentTimeMillis();
                return false;
            } else if (System.currentTimeMillis() > (beginTime + INTERVAL)) {
                punishCounter.set(0);
            }
        }
        return true;
    }


}
