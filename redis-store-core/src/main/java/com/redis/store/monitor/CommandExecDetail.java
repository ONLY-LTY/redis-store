package com.redis.store.monitor;

import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

class CommandExecDetail {

    static final int[] TIMEOUT_SCALE = new int[]{0, 10, 50, 100, 500};

    final ConcurrentMap<String, AtomicInteger[]> hostPortCommands = new ConcurrentSkipListMap<>(String::compareTo);

    void record(String hostPort, int duration) {
        int index = Arrays.binarySearch(TIMEOUT_SCALE, duration);
        if (index < 0) {
            index = -index - 2; // (-(-(insertion point) - 1) - 1) - 1
        }

        AtomicInteger[] counters = hostPortCommands.get(hostPort);
        if (counters == null) {
            counters = new AtomicInteger[TIMEOUT_SCALE.length];
            for (int i = 0; i < TIMEOUT_SCALE.length; ++i) {
                counters[i] = new AtomicInteger(0);
            }
            AtomicInteger[] _counters = hostPortCommands.putIfAbsent(hostPort, counters);
            if (_counters != null) {
                counters = _counters;
            }
        }
        counters[index].getAndIncrement();
    }
}
