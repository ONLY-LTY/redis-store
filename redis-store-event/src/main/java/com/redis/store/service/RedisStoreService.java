package com.redis.store.service;

import com.redis.store.client.ClusterClient;
import com.redis.store.cluster.Cluster;
import com.redis.store.constants.ClusterType;
import org.apache.log4j.Logger;


public class RedisStoreService extends AbstractClusterService {

    private static final Logger LOG = Logger.getLogger(RedisStoreService.class);

    private final ClusterClient clusterClient;

    /**
     * 默认读取"config.properties"配置文件
     */
    public RedisStoreService(String clientName, String clusterName) {
        this(clientName, clusterName, ClusterType.REDIS);
    }

    public RedisStoreService(String clientName, String clusterName, ClusterType clusterType) {
        super(clusterName, clientName, clusterType);

        clusterClient = new ClusterClient(clusterName, events, clusterType.getConfigRootPath());
        startEventHandler();
        LOG.info("RedisStoreService construction complete!");
    }

    @Override
    protected Cluster initCluster() {
        return clusterClient.loadCluster();
    }
}
