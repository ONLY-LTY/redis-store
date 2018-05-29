package com.redis.store.events;


import com.redis.store.cluster.Instance;
import com.redis.store.listener.InstanceChangeListener;

import java.util.List;

/**
 * luofucong on 14-8-20.
 */
public final class InstanceChangeEvent extends InstanceEvent {

    final Instance newInstance;

    final String nodeName;

    public InstanceChangeEvent(Instance newInstance, String nodeName) {
        this.newInstance = newInstance;
        this.nodeName = nodeName;
    }

    @Override
    public <T> void notify(List<T> listeners) {
        for (T listener : listeners) {
            ((InstanceChangeListener) listener).instanceDataChanged(newInstance, nodeName);
        }
    }

    @Override
    public String toString() {
        return "InstanceChangeEvent{" +
                "newInstance=" + newInstance +
                ", nodeName='" + nodeName + '\'' +
                '}';
    }
}
