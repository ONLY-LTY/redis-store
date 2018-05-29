package com.redis.store.proxy;


import com.redis.store.cluster.Instance;
import com.redis.store.cluster.Node;
import com.redis.store.cluster.NodeUtils;
import com.redis.store.constants.ClusterType;
import com.redis.store.constants.NodeStatus;
import com.redis.store.dao.IRedisDao;
import com.redis.store.monitor.JedisPoolConnMonitorTask;
import com.redis.store.util.JsonUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本类完成了从配置中心获取配置，创建连接池，以及预先创建一批链接的功能。
 * <p/>
 * 本类也是DAO寻找需要操作的node的处理类。DAO通过本类来获取具体的资源来进行读写
 * <p/>
 * 本类也支持配置的动态更新及推送
 *
 */
public class RedisConfigStoreProxy extends AbstractStoreProxy {

    private static final Logger logger = Logger.getLogger(RedisConfigStoreProxy.class);

    private static final Map<String, RedisConfigStoreProxy> CLUSTER_MAP = new ConcurrentHashMap<>();

    static {

        RetryCreateRedisDaoTask retryTask = new RetryCreateRedisDaoTask(CLUSTER_MAP, REDIS_DAO_MAP);
        retryTask.start();

        JedisPoolConnMonitorTask monitorTask = new JedisPoolConnMonitorTask(REDIS_DAO_MAP);
        monitorTask.start();
    }

    public synchronized static RedisConfigStoreProxy getProxyInstance(String clusterName) {
        return getProxyInstance(clusterName, null, 0);
    }

    public synchronized static RedisConfigStoreProxy getProxyInstance(String clusterName, JedisPoolConfig poolConfig, int poolInitConnNum) {
        return getProxyInstance(clusterName, poolConfig, poolInitConnNum, ClusterType.REDIS, null);
    }

    public synchronized static RedisConfigStoreProxy getProxyInstance(String clusterName, JedisPoolConfig poolConfig, int poolInitConnNum, ClusterType clusterType, String password) {
        if (!CLUSTER_MAP.containsKey(clusterName)) {
            RedisConfigStoreProxy proxy;
            try {
                proxy = new RedisConfigStoreProxy(clusterName, password, poolConfig, poolInitConnNum, 2000, clusterType);
            } catch (Exception e) {
                throw new RuntimeException("unable to create RedisConfigStoreProxy by cluster name "
                        + clusterName + " and type " + clusterType + ", using null object instead", e);
            }
            CLUSTER_MAP.put(clusterName, proxy);
        }
        return CLUSTER_MAP.get(clusterName);
    }


    private RedisConfigStoreProxy(String clusterName, String password/*cacheGroupId*/, JedisPoolConfig poolConfig, int poolInitConnNum, int timeout, ClusterType clusterType) {
        super(clusterName, password, poolConfig, poolInitConnNum, timeout, clusterType);
    }

    // 获取节点读写连接池
    public Map<String, List<IRedisDao>> getAllNodeDao(boolean ifRead) {
        Map<String, List<IRedisDao>> nodePoolMap = new HashMap<>();
        for (Node node : cluster.getNodeList()) {
            if (node.getStatus() != NodeStatus.ACTIVE)
                continue;

            List<Instance> instances;
            if (ifRead) {
                instances = NodeUtils.findReadInstance(node);
            } else {
                instances = NodeUtils.findWriteInstances(node);
            }
            if (CollectionUtils.isEmpty(instances)) {
                throw new IllegalArgumentException("the redis instance is not active...please check it: the key is:" + "the node is: " + node.getClusterName() + ":" + node.getName());
            }

            List<IRedisDao> redisDaoList = new ArrayList<>();
            for (Instance instance : instances) {
                if (getByHostPort(instance.findHostPort()) == null) {
                    continue;
                }
                redisDaoList.add(getByHostPort(instance.findHostPort()));
            }
            nodePoolMap.put(node.getName(), redisDaoList);
        }

        return nodePoolMap;
    }

    Map<String, List<IRedisDao>> getActiveSlaveInstances() {
        Map<String, List<IRedisDao>> activeSlaveInstances = new HashMap<>();
        for (Node node : cluster.getNodeList()) {
            if (node.getStatus() != NodeStatus.ACTIVE) {
                continue;
            }
            List<Instance> slaves = NodeUtils.getSlaves(node);
            List<IRedisDao> instances = new ArrayList<>(slaves == null ? 0 : slaves.size());
            if (slaves != null) {
                for (Instance slave : slaves) {
                    IRedisDao redisDao = getByHostPort(slave.findHostPort());
                    if (redisDao != null) {
                        instances.add(redisDao);
                    }
                }
            }
            activeSlaveInstances.put(node.getName(), instances);
        }
        return activeSlaveInstances;
    }

    public Node getNode(String hashKey) {
        Node node = hashLocator.getHashNode(hashKey);
        if (node == null) {
            throw new IllegalStateException("unable to locate node by hashkey: " + hashKey);
        }
        if (node.getStatus() != NodeStatus.ACTIVE) {
            throw new IllegalStateException("the node " + node.getClusterName() + "/" + node.getName() + " is not active");
        }
        return node;
    }

    Node getNodeByName(String nodeName) {
        for (Node node : cluster.getNodeList()) {
            if (node.getName().equals(nodeName)) {
                return node;
            }
        }
        return null;
    }

    public List<IRedisDao> getReadableRedisDaos(String hashkey, Node node) {
        List<Instance> instances = NodeUtils.findReadInstance(node);
        if (CollectionUtils.isEmpty(instances)) {
            throw new IllegalStateException("unable to find readable instances under node "
                    + node.getClusterName() + "/" + node.getName() + " located by hashkey " + hashkey);
        }
        return getRedisDaoByKey(hashkey, node, instances);
    }

    /**
     * 为了支持多写
     *
     * @param hashkey
     * @return
     */
    protected List<JedisPool> getShardedKeyWritePool(String hashkey) {
        List<IRedisDao> daoList = getShardWriteDao(hashkey);
        List<JedisPool> jedisPoolList = new ArrayList<>();
        for (IRedisDao IRedisDao : daoList) {
            jedisPoolList.add(IRedisDao.getJedisPool());
        }

        return jedisPoolList;
    }

    public List<IRedisDao> getShardWriteDao(String hashKey) {
        Node node = hashLocator.getHashNode(hashKey);
        if (node == null) {
            logger.error(" the hash node  is null the key is:" + hashKey);
            throw new IllegalStateException(
                    "the redis node is not exits...please check it: the key is:"
                            + hashKey);
        }

        if (node.getStatus() != NodeStatus.ACTIVE) {
            throw new IllegalStateException(
                    "the redis node is not active...please check it: the key is:"
                            + hashKey + "the node is: " + node.getClusterName()
                            + ":" + node.getName());
        }

        List<Instance> instances = NodeUtils.findWriteInstances(node);
        return getRedisDaoByKey(hashKey, node, instances);
    }

    public List<IRedisDao> getRedisDaoByKey(String hashKey, Node node, List<Instance> instances) {
        List<IRedisDao> redisDaoList = new ArrayList<>();
        Set<String> failFasts = new HashSet<>();
        for (Instance instance : instances) {
            IRedisDao redisDao = getByHostPort(instance.findHostPort());
            if (redisDao == null) {
                continue;
            }
            if (FailFast.getFailFastSwitch() && FailFast.isFailFast(instance.findHostPort())) {
                failFasts.add(instance.findHostPort());
                logger.error("the instance is in failFast status:" + instance.findHostPort());
                continue;
            }
            redisDaoList.add(redisDao);
        }

        if (redisDaoList.isEmpty()) {
            logger.error(" the instance redis dao is empty the key is:" + hashKey);
            throw new IllegalStateException(
                    "the redis dao is not exist...please check it: the key is:"
                            + hashKey + "the node is: " + node.getClusterName()
                            + ":" + node.getName()
                            + " the failFast instance is:"
                            + JsonUtils.toJSON(failFasts));
        }
        return redisDaoList;
    }

}
