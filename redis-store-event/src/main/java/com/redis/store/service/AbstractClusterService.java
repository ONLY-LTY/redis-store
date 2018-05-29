package com.redis.store.service;


import com.redis.store.client.PeerClient;
import com.redis.store.cluster.Cluster;
import com.redis.store.cluster.factory.AbstractClusterWrapper;
import com.redis.store.constants.ClusterType;
import com.redis.store.constants.ReHashStatus;
import com.redis.store.events.EventsHandler;
import com.redis.store.events.StoreEvent;
import com.redis.store.listener.ClusterChangedListener;
import com.redis.store.listener.InstanceChangeListener;
import com.redis.store.listener.NodeChangedListener;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

abstract class AbstractClusterService extends AbstractClusterWrapper {

    private static final int EVENT_EXECUTOR_POOL_SIZE = NumberUtils.toInt( System.getProperty("store.event-executor.pool-size"), 10);

    private static final int EVENT_EXECUTOR_RUN_INTERVAL_MS = NumberUtils.toInt( System.getProperty("store.event-executor.run-interval-ms"), 100);

    protected final LinkedBlockingQueue<StoreEvent> events = new LinkedBlockingQueue<>();

    private final PeerClient peerClient;

    private final List<ClusterChangedListener> clusterChangedListeners = new ArrayList<>();

    private final List<NodeChangedListener> nodeChangedListeners = new ArrayList<>();

    private final List<InstanceChangeListener> instanceChangeListeners = new ArrayList<>();

    private final ClusterType clusterType;

    private static final ScheduledExecutorService EVENTS_EXECUTOR = Executors.newScheduledThreadPool(
            EVENT_EXECUTOR_POOL_SIZE, new NamedThreadFactory("EventsHandler"));

    private volatile ScheduledFuture<?> eventsHandlerTask = null;

    /**
     * @param clusterName 配置集群名称
     * @param clientName  客户端实例名称
     */
    AbstractClusterService(String clusterName, String clientName, ClusterType clusterType) {
        super(clusterName, clientName);
        this.clusterType = clusterType;
        peerClient = new PeerClient(clusterType, clusterName, clientName, events);
    }

    void startEventHandler() {
        String clusterId = clusterType + "-" + clusterName;
        EventsHandler eventsHandler = new EventsHandler(clusterId, events, clusterChangedListeners, nodeChangedListeners, instanceChangeListeners);
        eventsHandlerTask = EVENTS_EXECUTOR.scheduleWithFixedDelay(eventsHandler, 1000, EVENT_EXECUTOR_RUN_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public Cluster loadCluster() {
        // 每次载入新集群时，丢弃上一次的集群变化事件
        events.clear();
        cluster = initCluster();
        return cluster;
    }

    protected abstract Cluster initCluster();

    @Override
    public void registerClusterChangedListener(ClusterChangedListener listener) {
        clusterChangedListeners.add(listener);
    }

    @Override
    public void registerNodeChangedListener(NodeChangedListener nodeListener) {
        nodeChangedListeners.add(nodeListener);
    }

    @Override
    public void registerInstanceChangedListener(InstanceChangeListener listener) {
        instanceChangeListeners.add(listener);
    }

    @Override
    public boolean sendClusterStatus(ReHashStatus status) {
        return peerClient.refreshStatus(status);
    }

    @Override
    public void close()  {
        if (eventsHandlerTask != null) {
            eventsHandlerTask.cancel(true);
        }
    }
}
