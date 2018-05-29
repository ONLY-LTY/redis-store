package com.redis.store.monitor;

import com.redis.store.service.NamedThreadFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


public class RedisStoreMonitor {

    private static final Reporter REPORTER = new Reporter();

    final String clusterName;

    AtomicReference<ConcurrentMap<String, CommandExecDetail>> commandExecDetails = new AtomicReference<>(new ConcurrentHashMap<>());

    static {
        Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("RedisStoreMonitor", true)).scheduleAtFixedRate(REPORTER, 1, 1, TimeUnit.SECONDS);
    }

    public RedisStoreMonitor(String clusterName) {
        this.clusterName = clusterName;
        REPORTER.register(this);
    }

    public void record(String command, Object[] args, Object result, String hostport, int duration) {
        ConcurrentMap<String, CommandExecDetail> details = commandExecDetails.get();
        CommandExecDetail detail = details.get(command);
        if (detail == null) {
            detail = new CommandExecDetail();
            CommandExecDetail _detail = details.putIfAbsent(command, detail);
            if (_detail != null) {
                detail = _detail;
            }
        }
        detail.record(hostport, duration);
    }
}
