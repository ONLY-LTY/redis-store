package com.redis.store.events;

import com.redis.store.cluster.Cluster;
import com.redis.store.listener.ClusterChangedListener;

import java.util.List;

public final class ClusterChangeEvent extends ClusterEvent {

    final Cluster newCluster;

    public ClusterChangeEvent(Cluster newCluster) {
        this.newCluster = newCluster;
    }

    @Override
    public <T> void notify(List<T> listeners) {
        for (T listener : listeners) {
            ((ClusterChangedListener) listener).clusterDataChanged(newCluster);
        }
    }

    @Override
    public String toString() {
        return "ClusterChangeEvent{" +
                "newCluster=" + newCluster +
                '}';
    }
}
