package com.redis.store.service;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author LTY
 * Date 2018/05/23
 */
public class NamedThreadFactory implements ThreadFactory {
    private final ThreadGroup threadGroup;
    private final String prefix;
    private final boolean isDaemon;
    private final AtomicInteger sequence;

    public NamedThreadFactory(String threadNamePrefix) {
        this(threadNamePrefix, false);
    }

    public NamedThreadFactory(String threadNamePrefix, boolean isDaemon) {
        this.sequence = new AtomicInteger(0);
        SecurityManager sm = System.getSecurityManager();
        this.threadGroup = sm == null ? Thread.currentThread().getThreadGroup() : sm.getThreadGroup();
        this.prefix = threadNamePrefix + "-thread-";
        this.isDaemon = isDaemon;
    }

    public Thread newThread(Runnable runnable) {
        String name = this.prefix + this.sequence.getAndIncrement();
        Thread thread = new Thread(this.threadGroup, runnable, name, 0L);
        thread.setDaemon(this.isDaemon);
        return thread;
    }
}
