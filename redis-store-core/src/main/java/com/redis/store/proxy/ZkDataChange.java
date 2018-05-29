package com.redis.store.proxy;

import com.redis.store.cluster.Cluster;
import com.redis.store.cluster.Instance;
import com.redis.store.cluster.Node;
import com.redis.store.cluster.NodeUtils;
import com.redis.store.constants.ActiveStatus;
import com.redis.store.constants.NodeStatus;
import com.redis.store.constants.ReHashStatus;
import com.redis.store.dao.IRedisDao;
import com.redis.store.listener.ClusterChangedListener;
import com.redis.store.listener.InstanceChangeListener;
import com.redis.store.listener.NodeChangedListener;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * Author LTY
 * Date 2018/05/24
 */
public class ZkDataChange implements ClusterChangedListener, NodeChangedListener, InstanceChangeListener {

    private static final Logger LOGGER = Logger.getLogger(ZkDataChange.class);

    private AbstractStoreProxy proxy;

    private Cluster newCluster = null;

    ZkDataChange(AbstractStoreProxy proxy) {
        this.proxy = proxy;
    }

    @Override
    public boolean instanceAdded(Instance newInstance, String nodeName) {
        LOGGER.info("receive the instanceAdded event, the nodeName is:" + nodeName);
        try {
            Node oldNode = proxy.getCluster().findNodeByName(nodeName);
            if (oldNode == null) {
                LOGGER.error("the node is null the name is:" + nodeName);
                return false;
            }

            proxy.initJedisPool(newInstance, false);
            Instance instance = NodeUtils.findInstanceByName(oldNode, newInstance.getName());
            if (instance == null) {
                oldNode.getInstances().add(newInstance);
            } else {
                oldNode.getInstances().remove(instance);
                oldNode.getInstances().add(newInstance);
            }
        } catch (Exception e) {
            LOGGER.error("fail to handle the instanceAdded event, the nodeName is:" + nodeName, e);
            return false;
        }
        return true;
    }

    @Override
    public boolean instanceDeleted(String instanceName, String nodeName) {
        LOGGER.info(" receive the instanceDeleted event ,the nodeName is:" + nodeName + " the instanceName is:" + instanceName);
        try {
            Node oldNode = proxy.getCluster().findNodeByName(nodeName);
            if (oldNode == null) {
                LOGGER.error("the node is null the nodeName is:" + nodeName);
                return false;
            }

            Instance oldInstance = NodeUtils.findInstanceByName(oldNode, instanceName);
            if (oldInstance == null) {
                LOGGER.error("the instance is null, the instanceName is:" + instanceName + " the nodeName is:" + nodeName);
                return false;
            }

            oldNode.getInstances().remove(oldInstance);
            proxy.removeJedisPoolByHostPort(oldInstance.findHostPort());
        } catch (Exception e) {
            LOGGER.error("fail to handle the instanceDeleted event. the nodeName is:" + nodeName, e);
        }
        LOGGER.info("finish to remove the instance  the instanceName is:" + instanceName + " the nodeName is:" + nodeName);
        return true;
    }

    @Override
    public boolean instanceDataChanged(Instance newInstance, String nodeName) {
        try {
            LOGGER.info("receive the instanceDataChanged  event, the nodeName is:" + nodeName + " the newInstance is:" + newInstance.toString());
            Node oldNode = proxy.getCluster().findNodeByName(nodeName);
            if (oldNode == null) {
                LOGGER.error("the node is null the name is:" + nodeName);
                return false;
            }
            Instance oldInstance = NodeUtils.findInstanceByName(oldNode, newInstance.getName());
            ActiveStatus oldStatus = oldInstance.getStatus();

            String hostPort = oldInstance.findHostPort();
            boolean isStatusChanged = (oldInstance.getStatus() != newInstance.getStatus());
            oldInstance.setAddTime(newInstance.getAddTime());
            oldInstance.setLastModifyTime(newInstance.getLastModifyTime());
            oldInstance.setMsStatus(newInstance.getMsStatus());
            oldInstance.setStatus(newInstance.getStatus());
            oldInstance.setHostName(newInstance.getHostName());
            oldInstance.setPort(newInstance.getPort());
            oldInstance.setReplicationState(newInstance.getReplicationState());

            if (!oldInstance.findHostPort().equals(newInstance.findHostPort())) {
                proxy.initJedisPool(newInstance, false);
                oldInstance.setDomain(newInstance.getDomain());
                oldInstance.setPort(newInstance.getPort());
                proxy.removeJedisPoolByHostPort(hostPort);
            } else if (isStatusChanged) {
                LOGGER.info("the instance status is changed  from " + oldStatus + " change to:" + newInstance.getStatus() + " the instance name is:" + newInstance.getName());
                //实例状态变更，对实例重建连接并替换，防止remove到rebuild之间异常
                if (newInstance.getStatus() != ActiveStatus.DELETED) {
                    IRedisDao redisDao = proxy.getByHostPort(newInstance.findHostPort());
                    proxy.initJedisPool(oldInstance, true);
                    proxy.removeJedisPoolByRedisDao(redisDao);
                    LOGGER.info("finish to rebuild the instance jedis pool: " + " the instance name is:" + newInstance.getName() + " the hostport is:" + newInstance.findHostPort());
                    proxy.resumePubsubChannels(newInstance, redisDao);
                } else {
                    proxy.removeJedisPoolByHostPort(newInstance.findHostPort());
                }
            }
        } catch (Exception e) {
            LOGGER.error("fail to handle the instanceDataChanged event, the nodeName is:" + nodeName, e);
        }
        return true;
    }

    @Override
    public boolean nodeAdd(Node newNode) {
        proxy.getCluster().addNode(newNode);
        return true;
    }

    @Override
    public boolean nodeDataChanged(Node newNode) {
        if (newNode == null)
            return false;
        LOGGER.info("receive the nodeDataChanged event the node name is:" + newNode.getName() + " the node is:" + newNode.toString());
        try {
            Node oldNode = proxy.getCluster().findNodeByName(newNode.getName());
            if (oldNode == null) {
                LOGGER.error("the node is null the name is:" + newNode.getName());
                return false;
            }

            boolean rehashFlag = (oldNode.getStatus() != newNode.getStatus());
            oldNode.setAddTime(newNode.getAddTime());
            oldNode.setLastModifyTime(newNode.getLastModifyTime());
            oldNode.setReadStrategy(newNode.getReadStrategy());
            oldNode.setWriteStrategy(newNode.getWriteStrategy());
            oldNode.setStatus(newNode.getStatus());
            oldNode.setStart(newNode.getStart());
            oldNode.setEnd(newNode.getEnd());
            oldNode.setWarmupBeginTime(newNode.getWarmupBeginTime());
            if (rehashFlag && newNode.getStatus() == NodeStatus.ACTIVE) {
                for (Instance instance : NodeUtils.findAliveInstances(newNode)) {
                    proxy.initJedisPool(instance, false);
                }
            }
        } catch (Exception e) {
            LOGGER.error("fail to handle the nodeDataChanged event the node is:" + newNode.toString(), e);
            return false;
        }
        return true;
    }

    @Override
    public boolean nodeDeleted(String nodeName) {
        LOGGER.info("receive the nodeDeleted event the nodeName is:" + nodeName + " the clusterName is:" + proxy.getCluster().getName());
        try {
            Node removedNode = proxy.getCluster().removeNodeByName(nodeName);
            List<Instance> instances = removedNode.getInstances();
            for (Instance instance : instances) {
                proxy.removeJedisPoolByHostPort(instance.findHostPort());
            }
        } catch (Exception e) {
            LOGGER.error("fail to close the instance conn... the error is:", e);
        }
        proxy.clusterRehash();
        return true;
    }

    @Override
    public synchronized void rehash(ReHashStatus newStatus) {
        LOGGER.info("receive the rehash event the status is:" + newStatus.toString());
        try {
            if (newStatus == ReHashStatus.SYN) {
                newCluster = proxy.loadClusterConfig();
                if (newCluster == null) {
                    LOGGER.error("the new cluster is null....");
                    proxy.sendClusterStatus(ReHashStatus.FAIL);
                    return;
                }
                proxy.setStatus(newStatus);
                proxy.initCluster(newCluster);
                proxy.sendClusterStatus(ReHashStatus.ACK);
                proxy.setStatus(ReHashStatus.ACK);
            } else if (newStatus == ReHashStatus.REHASH) {
                if (newCluster == null) {
                    LOGGER.error("the new cluster is null....");
                    proxy.sendClusterStatus(ReHashStatus.FAIL);
                    return;
                }
                // confirm the cluster finish to init conn ...
                Thread.sleep(5000);
                proxy.setCluster(newCluster);
                proxy.clusterRehash();
                newCluster = null;

                proxy.sendClusterStatus(ReHashStatus.FINISHED);
                proxy.setStatus(ReHashStatus.FINISHED);
                Thread.sleep(5000);
                proxy.sendClusterStatus(ReHashStatus.NORMAL);
                proxy.setStatus(ReHashStatus.NORMAL);
            }
        } catch (Exception e) {
            LOGGER.error("fail to handle the rehash event the error is:", e);
            proxy.sendClusterStatus(ReHashStatus.FAIL);
        }
    }

    @Override
    public void clusterDataChanged(Cluster newCluster) {
        LOGGER.info("receive the clusterDataChanged event...");

        boolean ifShardStrategyChanged = (proxy.getCluster().getShardStrategy() != newCluster.getShardStrategy());
        proxy.getCluster().setAddTime(newCluster.getAddTime());
        proxy.getCluster().setLastModifyTime(newCluster.getLastModifyTime());
        proxy.getCluster().setName(newCluster.getName());
        proxy.getCluster().setStatus(newCluster.getStatus());
        proxy.getCluster().setNodeWarmupDurationSec(newCluster.getNodeWarmupDurationSec());

        if (ifShardStrategyChanged) {
            LOGGER.info("the cluster shard strategy is changed... change from:" + proxy.getCluster().getShardStrategy() + " to:" + newCluster.getShardStrategy());
            // 暂时不开启实时更改cluster的hash策略
            // cluster.setShardStrategy(newCluster.getShardStrategy());
            // clusterRehash();
        }

    }
}

