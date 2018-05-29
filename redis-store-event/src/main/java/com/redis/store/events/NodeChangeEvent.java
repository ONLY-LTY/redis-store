package com.redis.store.events;


import com.redis.store.cluster.Node;
import com.redis.store.listener.NodeChangedListener;

import java.util.List;

public final class NodeChangeEvent extends NodeEvent {

    final Node newNode;

    public NodeChangeEvent(Node newNode) {
        this.newNode = newNode;
    }

    @Override
    public <T> void notify(List<T> listeners) {
        for (T listener : listeners) {
            ((NodeChangedListener) listener).nodeDataChanged(newNode);
        }
    }

    @Override
    public String toString() {
        return "NodeChangeEvent{" +
                "newNode=" + newNode +
                '}';
    }
}
