package com.redis.store.proxy;

import com.redis.store.cluster.Cluster;
import com.redis.store.cluster.Instance;
import com.redis.store.cluster.Node;
import com.redis.store.cluster.NodeUtils;
import com.redis.store.constants.NodeStatus;
import com.redis.store.dao.IRedisDao;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RetryCreateRedisDaoTask implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(RetryCreateRedisDaoTask.class);

    private static final long CREATE_REDIS_DAO_TASK_DELAY = 3; // MINUTES
    private static final long CREATE_REDIS_DAO_TASK_PERIOD = 1;// MINUTES

    private Map<String, RedisConfigStoreProxy> clusterMap;
    private Map<String, IRedisDao> redisDaoMap;

    public RetryCreateRedisDaoTask( Map<String, RedisConfigStoreProxy> clusterMap, Map<String, IRedisDao> redisDaoMap) {
        this.clusterMap = clusterMap;
        this.redisDaoMap = redisDaoMap;
    }

    public void start() {
        ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor();
        es.scheduleAtFixedRate(this, CREATE_REDIS_DAO_TASK_DELAY, CREATE_REDIS_DAO_TASK_PERIOD, TimeUnit.MINUTES);
    }

    @Override
    public void run() {
        try {
            Iterator<Entry<String, RedisConfigStoreProxy>> iterator = clusterMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, RedisConfigStoreProxy> entry = iterator.next();
                RedisConfigStoreProxy configProxy = entry.getValue();
                if (configProxy == null) {
                    LOGGER.warn("remove not existed cluster " + entry.getKey());
                    iterator.remove();
                    continue;
                }
                Cluster cluster = configProxy.getCluster();
                for (Node node : cluster.getNodeList()) {
                    if (node.getStatus() != NodeStatus.ACTIVE) {
                        continue;
                    }
                    for (Instance instance : NodeUtils.findActiveInstances(node)) {
                        if (redisDaoMap.get(instance.findHostPort()) == null) {
                            try {
                                configProxy.initJedisPool(instance, false);
                            } catch (Exception e) {
                                LOGGER.error("", e);
                            }
                        }
                    }
                }
            }

        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }
}
