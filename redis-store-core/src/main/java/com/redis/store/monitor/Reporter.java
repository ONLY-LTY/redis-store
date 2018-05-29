package com.redis.store.monitor;

import com.redis.store.utils.LogCreator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.redis.store.monitor.CommandExecDetail.TIMEOUT_SCALE;


class Reporter implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(Reporter.class);

    private static final String OUTPUT_FORMAT;

    private static final String TITLE;

    static {

        OUTPUT_FORMAT = Arrays.stream(TIMEOUT_SCALE).mapToObj(scale -> "%-10s  ").collect(Collectors.joining());

        Object[] titleTokens = new String[TIMEOUT_SCALE.length];
        for (int i = 0, l = TIMEOUT_SCALE.length; i < l; i++) {
            String scale1 = String.valueOf(TIMEOUT_SCALE[i]);
            String scale2 = i + 1 < l ? String.valueOf(TIMEOUT_SCALE[i + 1]) : "Inf";
            titleTokens[i] = "[" + scale1 + "," + scale2 + ")";
        }
        TITLE = String.format("%-70s", "HOST_PORT") + String.format(OUTPUT_FORMAT, titleTokens);
    }

    private final Map<String, Logger> commandDetailLoggers = new HashMap<>();

    private final Set<RedisStoreMonitor> redisStoreMonitors = Collections.newSetFromMap(new ConcurrentHashMap<RedisStoreMonitor, Boolean>());

    void register(RedisStoreMonitor redisStoreMonitor) {
        redisStoreMonitors.add(redisStoreMonitor);
    }

    @Override
    public void run() {
        try {
            for (RedisStoreMonitor monitor : redisStoreMonitors) {
                report(monitor);
            }
        } catch (Exception e) {
            LOGGER.error("Ops", e);
        }
    }

    private void report(RedisStoreMonitor monitor) {
        ConcurrentMap<String, CommandExecDetail> details = monitor.commandExecDetails.get();
        if (!monitor.commandExecDetails.compareAndSet(details, new ConcurrentHashMap<>())) {
            return;
        }

        for (Map.Entry<String, CommandExecDetail> entry : details.entrySet()) {
            String command = entry.getKey();
            CommandExecDetail detail = entry.getValue();
            ConcurrentMap<String, AtomicInteger[]> hostPortCommands = detail.hostPortCommands;
            logMetrics(monitor, command, hostPortCommands);
        }
    }

    private void logMetrics(RedisStoreMonitor monitor, String command, ConcurrentMap<String, AtomicInteger[]> hostPortCommands) {
        Logger logger = getMonitorLogger(command, monitor.clusterName);
        logger.info("");

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        int second = calendar.get(Calendar.SECOND);
        if (second % 10 == 0) {
            logger.info(monitor.clusterName + "-" + command);
        }
        logger.info(DateFormatUtils.format(calendar, "yyyy-MM-dd HH:mm:ss"));
        logger.info(TITLE);

        for (Map.Entry<String, AtomicInteger[]> entry : hostPortCommands.entrySet()) {
            String hostPort = entry.getKey();
            AtomicInteger[] counters = entry.getValue();
            logger.info(String.format("%-70s", hostPort) + String.format(OUTPUT_FORMAT, (Object[]) counters));
        }
    }

    private Logger getMonitorLogger(String command, String clusterName) {
        Logger logger = commandDetailLoggers.get(command);
        if (logger == null) {
            synchronized (command.intern()) {
                logger = commandDetailLoggers.get(command);
                if (logger == null) {
                    String logPath = System.getProperty("redisLogPath", "worker");
                    if (StringUtils.isNotEmpty(logPath)) {
                        logPath = StringUtils.appendIfMissing(logPath, "/");
                    }
                    logger = LogCreator.createLogger(clusterName + "-" + command, logPath + clusterName);
                    commandDetailLoggers.put(command, logger);
                }
            }
        }
        return logger;
    }
}
