package com.redis.store.proxy.ha;

import com.google.common.collect.Maps;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * @author yangzhongjie
 *         Created:2016-11-28
 */
abstract class IsolatorStrategy {

    protected static final Logger LOG = Logger.getLogger(IsolatorStrategy.class);

    private final ConcurrentMap<String, Isolator> isolators = Maps.newConcurrentMap();

    void register(Isolator isolator) {
        isolators.putIfAbsent(isolator.getHostPort(), isolator);
        LOG.error("Register isolator name : " + isolator.getHostPort());
    }

    void recover(String name) {
        Isolator isolator = isolators.get(name);
        if (isolator != null) {
            isolator.setState(Isolator.State.RECOVERED);
            LOG.error("Recover isolator name : " + name);
        }
    }

    int isolatorsSize() {
        return isolators.size();
    }

    Map<String, Isolator> getIsolators() {
        return Collections.unmodifiableMap(isolators);
    }

    boolean isReleased(String name) {
        Isolator isolator = isolators.get(name);
        if (isolator == null || isolator.getState() == Isolator.State.RECOVERED) {
            return true;
        }
        return processor(isolator);
    }

    abstract boolean processor(Isolator isolator);

}
