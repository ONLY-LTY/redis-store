package com.redis.store.events;

import com.redis.store.constants.ReHashStatus;
import com.redis.store.listener.ClusterChangedListener;

import java.util.List;

public final class ClusterRehashEvent extends ClusterEvent {

    final ReHashStatus reHashStatus;

    public ClusterRehashEvent(ReHashStatus reHashStatus) {
        this.reHashStatus = reHashStatus;
    }

    @Override
    public <T> void notify(List<T> listeners) {
        for (T listener : listeners) {
            ((ClusterChangedListener) listener).rehash(reHashStatus);
        }
    }

    @Override
    public String toString() {
        return "ClusterRehashEvent{" +
                "reHashStatus=" + reHashStatus +
                '}';
    }
}
