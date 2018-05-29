package com.redis.store.events;


import com.redis.store.listener.NodeChangedListener;

import java.util.List;

/**
 * luofucong on 14-8-20.
 */
public final class NodeRemoveEvent extends NodeEvent {

    final String nodeName;

    public NodeRemoveEvent(String nodeName) {
        this.nodeName = nodeName;
    }

    @Override
    public <T> void notify(List<T> listeners) {
        for (T listener : listeners) {
            ((NodeChangedListener) listener).nodeDeleted(nodeName);
        }
    }

    @Override
    public String toString() {
        return "NodeRemoveEvent{" +
                "nodeName='" + nodeName + '\'' +
                '}';
    }
}
