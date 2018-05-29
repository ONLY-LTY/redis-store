package com.redis.store.service;

import com.redis.store.cluster.Instance;
import com.redis.store.cluster.Node;
import com.redis.store.constants.ClusterType;
import com.redis.store.constants.MasterSlaveStatus;
import com.redis.store.constants.NodeReadStrategy;
import com.redis.store.exception.NoClusterOrNodeException;
import com.redis.store.util.JsonUtils;
import com.redis.store.util.ZookeeperUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.File;
import java.util.Date;

import static org.apache.curator.utils.ZKPaths.makePath;


public class RegistryService {

    private static final Logger LOGGER = Logger.getLogger(RegistryService.class);

    /**
     * 注册Node
     *
     * @param clusterName 集群名
     * @param clusterType 集群类型
     * @param node        Node实例
     * @param overwrite   是否覆盖上一次注册的Node
     * @param ephemeral   是否临时节点
     * @throws NoClusterOrNodeException 集群未创建
     */
    public static void registerNode(String clusterName, ClusterType clusterType, Node node, boolean overwrite, boolean ephemeral)
            throws NoClusterOrNodeException {
        String path = clusterType.getConfigRootPath() + File.separator + clusterName + File.separator + node.getName();
        String nodeStr = JsonUtils.toJSON(node);
        byte[] bytes = nodeStr.getBytes();

        try {
            if (update(path, bytes, overwrite, ephemeral)) {
                LOGGER.info("Register node " + nodeStr + " under " + path + " succeeds!");
            }
        } catch (KeeperException.NoNodeException e) {
            throw new NoClusterOrNodeException(clusterType, clusterName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 注册Instance
     *
     * @param clusterName 集群名
     * @param clusterType 集群类型
     * @param nodeName    Node名
     * @param instance    Instance实例
     * @param overwrite   是否覆盖上一次注册的Instance
     * @param ephemeral   是否临时节点
     * @throws NoClusterOrNodeException 集群或Node未创建
     */
    public static void registerInstance(String clusterName, ClusterType clusterType, String nodeName,
                                        final Instance instance, final boolean overwrite, final boolean ephemeral)
            throws NoClusterOrNodeException {
        String nodePath = clusterType.getConfigRootPath() + File.separator + clusterName + File.separator + nodeName;
        final String instancePath = nodePath + File.separator + instance.getName();
        final String instanceStr = JsonUtils.toJSON(instance);
        final byte[] bytes = instanceStr.getBytes();

        try {
            if (update(instancePath, bytes, overwrite, ephemeral)) {
                LOGGER.info("Register instance " + instanceStr + " under " + instancePath + " succeeds!");

                CuratorFramework zookeeperClient = ZookeeperUtils.getZookeeperClient();
                Stat stat = zookeeperClient.checkExists().forPath(nodePath);
                if (stat != null) {
                    byte[] nodeJsonRaw = zookeeperClient.getData().forPath(nodePath);
                    if (nodeJsonRaw != null) {
                        Node node = JsonUtils.toT(new String(nodeJsonRaw), Node.class);
                        node.setLastModifyTime(new Date());
                        node.setWarmupBeginTime(System.currentTimeMillis());
                        if (instance.getMsStatus() == MasterSlaveStatus.SLAVE && clusterType == ClusterType.ONESTORE) {
                            node.setReadStrategy(NodeReadStrategy.RANDOM);
                        }
                        byte[] nodeJsonNew = JsonUtils.toJSON(node).getBytes();
                        zookeeperClient.setData().withVersion(stat.getVersion()).forPath(nodePath, nodeJsonNew);
                    }

                    zookeeperClient.getConnectionStateListenable().addListener((client, newState) -> {
                        LOGGER.warn("zookeeper connection new state: " + newState);
                        if (newState == ConnectionState.RECONNECTED) {
                            try {
                                boolean updated = update(instancePath, bytes, overwrite, ephemeral);
                                LOGGER.info("Re-register instance " + instanceStr + " under " + instancePath + (updated ? " succeeded!" : " failed!"));
                            } catch (Exception e) {
                                LOGGER.error("Ops", e);
                            }
                        }
                    });
                } // else ignore.
            }
        } catch (KeeperException.NoNodeException e) {
            throw new NoClusterOrNodeException(clusterType, clusterName + File.separator + nodeName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void unRegisterInstance(String clusterName, ClusterType clusterType, String nodeName,
                                          String instanceName) {
        CuratorFramework zookeeperClient = ZookeeperUtils.getZookeeperClient();
        try {
            zookeeperClient.delete().forPath(makePath(clusterType.getConfigRootPath(), clusterName, nodeName, instanceName));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean update(String path, byte[] bytes, boolean overwrite, boolean ephemeral) throws Exception {
        CuratorFramework zookeeperClient = ZookeeperUtils.getZookeeperClient();
        Stat stat = zookeeperClient.checkExists().forPath(path);

        boolean updated = true;
        if (stat != null) {
            if (overwrite) {
                try {
                    zookeeperClient.delete().forPath(path);
                } catch (KeeperException.NoNodeException ignored) {
                    LOGGER.error(ignored);
                }
                if (ephemeral) {
                    zookeeperClient.create().withMode(CreateMode.EPHEMERAL).forPath(path, bytes);
                } else {
                    zookeeperClient.create().forPath(path, bytes);
                }
            } else {
                updated = false;
            }
        } else {
            try {
                CreateBuilder create = zookeeperClient.create();
                if (ephemeral) {
                    create.withMode(CreateMode.EPHEMERAL);
                }
                create.forPath(path, bytes);
            } catch (KeeperException.NodeExistsException e) {
                if (overwrite) {
                    zookeeperClient.setData().forPath(path, bytes);
                } else {
                    updated = false;
                }
            }
        }
        return updated;
    }
}
