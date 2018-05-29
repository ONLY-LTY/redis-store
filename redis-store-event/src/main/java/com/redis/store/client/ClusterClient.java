package com.redis.store.client;

import com.redis.store.cluster.Cluster;
import com.redis.store.cluster.Instance;
import com.redis.store.cluster.Node;
import com.redis.store.cluster.NodeUtils;
import com.redis.store.events.*;
import com.redis.store.util.JsonUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.redis.store.constants.ClusterType.REDIS;
import static com.redis.store.util.CommonUtils.str;
import static com.redis.store.util.CommonUtils.stripName;
import static com.redis.store.util.ZkClientUtils.getZkClient;


/**
 * 监控zookeeper上节点配置信息的变化
 */
public class ClusterClient {

    private static final Logger LOG = Logger.getLogger(ClusterClient.class);

    private static final Logger ZK_EVENTS_LOG = Logger.getLogger("ZkEvents");

    private final String clusterPath;

    private final LinkedBlockingQueue<StoreEvent> events;

    // 本地数据是否初始化完成
    private volatile boolean initialized = false;

    private ConcurrentMap<String, Set<String>> clusterSkeleton;

    public ClusterClient(String clusterName, LinkedBlockingQueue<StoreEvent> events) {
        this(clusterName, events, REDIS.getConfigRootPath());
    }
    
    public ClusterClient(String clusterName, LinkedBlockingQueue<StoreEvent> events, String clusterRootPath) {
        this.clusterPath = clusterRootPath + File.separator + clusterName;
        this.events = events;
        subscribeAll();
    }

    private void subscribeAll() {
        subscribeCluster();
        // 监听所有Node
        List<String> nodeNames = getZkClient().getChildren(clusterPath);
        for (String nodeName : nodeNames) {
            nodeName = stripName(nodeName);
            subscribeNode(nodeName);
        }
        // 监听所有的Instance
        for (String nodeName : nodeNames) {
            nodeName = stripName(nodeName);
            List<String> instanceNames = getZkClient().getChildren(clusterPath + File.separator + nodeName);
            for (String instanceName : instanceNames) {
                instanceName = stripName(instanceName);
                subscribeInstance(nodeName, instanceName);
            }
        }
    }

    public synchronized Cluster loadCluster() {
        initialized = false;

        Cluster cluster = initCluster();
        initNodesAndInstances(cluster);

        initialized = true;
        return cluster;
    }

    private Cluster initCluster() {
        String clusterPropJson;
        if ((clusterPropJson = getZkClient().readData(clusterPath, true)) == null) {
            throw new IllegalArgumentException("path not found " + clusterPath);
        }
        return JsonUtils.toT(clusterPropJson, Cluster.class);
    }

    private void initNodesAndInstances(Cluster cluster) {
        List<String> nodeNames = getZkClient().getChildren(clusterPath);
        // 初始化节点和实例
        for (String nodeName : nodeNames) {
            nodeName = stripName(nodeName);
            Node node = cluster.findNodeByName(nodeName);
            if (node != null) {
                throw new IllegalStateException("BAD DATA: duplicate node name " + nodeName);
            }
            String nodePath = clusterPath + File.separator + nodeName;
            String nodePropJson = getZkClient().readData(nodePath, true);
            node = JsonUtils.toT(nodePropJson, Node.class);

            List<String> instanceNames = getZkClient().getChildren(nodePath);
            for (String instanceName : instanceNames) {
                instanceName = stripName(instanceName);
                Instance instance = NodeUtils.findInstanceByName(node, instanceName);
                if (instance != null) {
                    throw new IllegalStateException("BAD DATA: duplicate instance name " + instanceName);
                }
                String instancePath = nodePath + File.separator + instanceName;
                String instancePropJson = getZkClient().readData(instancePath, true);
                instance = JsonUtils.toT(instancePropJson, Instance.class);

                node.addInstance(instance);
            }

            cluster.addNode(node);
        }

        extractClusterSkeleton(cluster);
    }

    private void extractClusterSkeleton(Cluster cluster) {
        clusterSkeleton = new ConcurrentHashMap<>();

        List<Node> nodes = cluster.getNodeList();
        for (Node node : nodes) {
            HashSet<String> instances = new HashSet<>();
            clusterSkeleton.put(node.getName(), instances);

            List<Instance> instanceList = node.getInstances();
            for (Instance instance : instanceList) {
                instances.add(instance.getName());
            }
        }
    }

    private void subscribeCluster() {
        // 监听Node加入事件
        getZkClient().subscribeChildChanges(clusterPath, (parentPath, currentChilds) -> {
            ZK_EVENTS_LOG.info("[Cluster][Children] " + parentPath + " " + str(currentChilds));

            // 仅考虑Node被加入的情况. Node被删除或者Node被更新则在另外的listener中处理
            if (CollectionUtils.isEmpty(currentChilds)) return;

            checkAndWait();

            for (String nodeName : currentChilds) {
                nodeName = stripName(nodeName);
                if (!clusterSkeleton.containsKey(nodeName)) {
                    String nodePropJson = getZkClient().readData(clusterPath + File.separator + nodeName, true);
                    Node newNode = JsonUtils.toT(nodePropJson, Node.class);
                    if (newNode != null) {
                        clusterSkeleton.put(nodeName, new HashSet<>());

                        events.offer(new NodeAddEvent(newNode));

                        // subscribe to new node
                        subscribeNode(nodeName);
                    }
                }
            }
        });

        getZkClient().subscribeDataChanges(clusterPath, new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) {
                ZK_EVENTS_LOG.info("[Cluster][Change] " + dataPath + " " + data);
                Cluster newCluster = JsonUtils.toT(data.toString(), Cluster.class);
                if (newCluster != null) {
                    events.offer(new ClusterChangeEvent(newCluster));
                }
            }

            @Override
            public void handleDataDeleted(String dataPath) {
                ZK_EVENTS_LOG.info("[Cluster][Remove] " + dataPath);
                // 此方法不该被调用的. 集群被删除了?
            }
        });
    }

    private void subscribeNode(final String nodeName) {
        final String nodePath = clusterPath + File.separator + nodeName;
        // 监听实例被加入某个Node的事件
        getZkClient().subscribeChildChanges(nodePath, (parentPath, currentChilds) -> {
            ZK_EVENTS_LOG.info("[Node][Children] " + parentPath + " " + str(currentChilds));

            // 仅考虑实例被加入的情况. 实例被删除或者实例被更新则在另外的listener中处理
            if (CollectionUtils.isEmpty(currentChilds)) return;

            checkAndWait();

            Set<String> instances = clusterSkeleton.get(nodeName);
            if (instances == null) {
                return;
            }
            for (String instanceName : currentChilds) {
                instanceName = stripName(instanceName);
                if (!instances.contains(instanceName)) {
                    // 尝试获取实例的配置. 有可能为null, 或者不是最终的配置(会由其他的listener更新)
                    String instancePropJson = getZkClient().readData(nodePath + File.separator + instanceName);
                    Instance instance = JsonUtils.toT(instancePropJson, Instance.class);
                    if (instance != null) {
                        instances.add(instanceName);

                        events.offer(new InstanceAddEvent(nodeName, instance));

                        // subscribe to new instance
                        subscribeInstance(nodeName, instanceName);
                    }
                }
            }
        });
        getZkClient().subscribeDataChanges(nodePath, new IZkDataListener() {
            // 监听Node配置信息变化的事件
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                ZK_EVENTS_LOG.info("[Node][Change] " + dataPath + " " + data);

                if (isNodeNotExisted()) return;

                Node node = JsonUtils.toT(data.toString(), Node.class);
                if (node != null) {
                    events.offer(new NodeChangeEvent(node));
                }
            }

            // 监听Node删除事件
            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                ZK_EVENTS_LOG.info("[Node][Remove] " + dataPath);

                if (isNodeNotExisted()) return;

                clusterSkeleton.remove(nodeName);

                events.offer(new NodeRemoveEvent(nodeName));

                getZkClient().unsubscribeDataChanges(nodePath, this);
            }

            private boolean isNodeNotExisted() {
                return !clusterSkeleton.containsKey(nodeName);
            }
        });
    }

    private void subscribeInstance(final String nodeName, final String instanceName) {
        final String instancePath = clusterPath + File.separator + nodeName + File.separator + instanceName;
        getZkClient().subscribeDataChanges(instancePath, new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                ZK_EVENTS_LOG.info("[Instance][Change] " + dataPath + " " + data);

                if (isInstanceNotExisted()) return;

                Instance instance = JsonUtils.toT(data.toString(), Instance.class);
                if (instance != null) {
                    events.offer(new InstanceChangeEvent(instance, nodeName));
                }
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                ZK_EVENTS_LOG.info("[Instance][Remove] " + dataPath);

                if (isInstanceNotExisted()) return;

                events.offer(new InstanceRemoveEvent(instanceName, nodeName));

                getZkClient().unsubscribeDataChanges(instancePath, this);
                
                //remove the instance cache data
                clusterSkeleton.get(nodeName).remove(instanceName);
            }

            private boolean isInstanceNotExisted() {
                return !clusterSkeleton.containsKey(nodeName)
                        || !clusterSkeleton.get(nodeName).contains(instanceName);
            }
        });
    }

    private void checkAndWait() {
        while (!initialized) {
            try {
                LOG.info("Wait for cluster init.");
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                LOG.error("Ops", e);
            }
        }
    }
}
