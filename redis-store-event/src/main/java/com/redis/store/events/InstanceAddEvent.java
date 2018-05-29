package com.redis.store.events;


import com.redis.store.cluster.Instance;
import com.redis.store.listener.InstanceChangeListener;

import java.util.List;

public final class InstanceAddEvent extends InstanceEvent {

    final String nodeName;

    final Instance newInstance;

    public InstanceAddEvent(String nodeName, Instance newInstance) {
        this.nodeName = nodeName;
        this.newInstance = newInstance;
    }

    @Override
    public <T> void notify(List<T> listeners) {
        for (T listener : listeners) {
            ((InstanceChangeListener) listener).instanceAdded(newInstance, nodeName);
        }
    }

    @Override
    public String toString() {
        return "InstanceAddEvent{" +
                "nodeName='" + nodeName + '\'' +
                ", newInstance=" + newInstance +
                '}';
    }
}
