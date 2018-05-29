package com.redis.store.events;


import com.redis.store.listener.InstanceChangeListener;

import java.util.List;


public final class InstanceRemoveEvent extends InstanceEvent {

    final String instanceName;

    final String nodeName;

    public InstanceRemoveEvent(String instanceName, String nodeName) {
        this.instanceName = instanceName;
        this.nodeName = nodeName;
    }

    @Override
    public <T> void notify(List<T> listeners) {
        for (T listener : listeners) {
            ((InstanceChangeListener) listener).instanceDeleted(instanceName, nodeName);
        }
    }

    @Override
    public String toString() {
        return "InstanceRemoveEvent{" +
                "instanceName='" + instanceName + '\'' +
                ", nodeName='" + nodeName + '\'' +
                '}';
    }
}
