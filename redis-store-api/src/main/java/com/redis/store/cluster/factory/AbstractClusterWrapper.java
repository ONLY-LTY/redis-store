package com.redis.store.cluster.factory;

import com.redis.store.cluster.Cluster;
import com.redis.store.constants.ReHashStatus;
import com.redis.store.listener.ClusterChangedListener;
import com.redis.store.listener.InstanceChangeListener;
import com.redis.store.listener.NodeChangedListener;

import java.io.Closeable;

/**
 * @author LTY
 */
public abstract class AbstractClusterWrapper implements Closeable {

    protected volatile Cluster cluster;

    protected final String clusterName;

    protected final String clientName;

    /**
     * @param clientName  客户端实例名称
     * @param clusterName 配置集群名称
     */
    public AbstractClusterWrapper(String clusterName, String clientName) {
        this.clusterName = clusterName;
        this.clientName = clientName;
    }

    public abstract Cluster loadCluster();

    public abstract void registerClusterChangedListener( ClusterChangedListener listener);

    public abstract void registerNodeChangedListener( NodeChangedListener nodeListener);

    public abstract void registerInstanceChangedListener( InstanceChangeListener listener);

    public abstract boolean sendClusterStatus(ReHashStatus status);
}
