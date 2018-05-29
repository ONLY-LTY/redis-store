package com.redis.store.events;


import com.redis.store.cluster.Node;
import com.redis.store.listener.NodeChangedListener;

import java.util.List;

/**
 * luofucong on 14-8-20.
 */
public final class NodeAddEvent extends NodeEvent {

    final Node newNode;

    public NodeAddEvent(Node newNode) {
        this.newNode = newNode;
    }

    @Override
    public <T> void notify(List<T> listeners) {
        for (T listener : listeners) {
            ((NodeChangedListener) listener).nodeAdd(newNode);
        }
    }

    @Override
    public String toString() {
        return "NodeAddEvent{" +
                "newNode=" + newNode +
                '}';
    }
}
