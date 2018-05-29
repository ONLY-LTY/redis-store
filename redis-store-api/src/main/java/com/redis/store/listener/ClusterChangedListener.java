package com.redis.store.listener;

import com.redis.store.cluster.Cluster;
import com.redis.store.constants.ReHashStatus;

public interface ClusterChangedListener {

    public void clusterDataChanged(Cluster newCluster);

    public void rehash(ReHashStatus newStatus);
}
