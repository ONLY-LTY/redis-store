package com.redis.store.service;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.redis.store.cluster.Cluster;
import com.redis.store.cluster.Instance;
import com.redis.store.cluster.Node;
import com.redis.store.cluster.NodeUtils;
import com.redis.store.cluster.factory.IClusterManager;
import com.redis.store.constants.ClusterType;
import com.redis.store.constants.ReHashStatus;
import com.redis.store.util.JsonUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.*;

import static com.redis.store.constants.ClusterType.REDIS;
import static com.redis.store.util.CommonUtils.stripName;
import static com.redis.store.util.ZkClientUtils.getZkClient;


public class ClusterManager implements IClusterManager {

    private static final String MONITOR_PATH = "/cluster_to_monitor";
    private static final String TYPE = "__type__";

    public static final ClusterManager INSTANCE = new ClusterManager();

    private String clusterRootPath = REDIS.getConfigRootPath();

    private ClusterType clusterType;

    private Logger logger = Logger.getLogger(ClusterManager.class);

    public ClusterManager(ClusterType clusterType) {
        this.clusterType = clusterType;
        clusterRootPath = clusterType.getConfigRootPath();
        initClusterRootPath();
    }

    // default for redis cluster config manager
    public ClusterManager() {
        this(REDIS);
    }

    private void initClusterRootPath() {
        try {
            if (!getZkClient().exists(clusterRootPath)) {
                getZkClient().createPersistent(clusterRootPath, this.clusterType.toString());
            }
        } catch (Exception e) {
            logger.error("fail to create the clusterRootPath:" + clusterRootPath, e);
        }
    }

    @Override
    public Cluster findClusterByName(String clusterName, boolean isFetchingNodes) {
        if (StringUtils.isEmpty(clusterName)) {
            return null;
        }
        Cluster cluster = null;
        String clusterPropJson = getZkClient().readData(
                clusterRootPath + File.separator + clusterName, true);
        if (StringUtils.isNotBlank(clusterPropJson)) {
            cluster = JsonUtils.toT(clusterPropJson, Cluster.class);
            if (isFetchingNodes) {
                initNodes(clusterName, cluster);
            }
        }
        return cluster;
    }

    public List<Cluster> findClustersByNamePrefix(final String pattern, int limit) {

        return myFindClustersByNamePrefix(limit, input -> {
            if (input == null) {
                return false;
            }
            return input.matches(pattern);
        }, true);
    }

    @Override
    public List<Cluster> getMatchingClusters(final String pattern, boolean initNodes) {
        Function<String, Boolean> func = input -> input != null && input.matches(pattern);
        return myFindClustersByNamePrefix(-1, func, initNodes);
    }

    private List<Cluster> myFindClustersByNamePrefix(int limit, Function<String, Boolean> func, boolean initNodes) {
        List<Cluster> clusterList = new ArrayList<>();
        List<String> clusterNames = getZkClient().getChildren(clusterRootPath);
        if (CollectionUtils.isNotEmpty(clusterNames)) {
            int found = 0;
            for (String clusterName : clusterNames) {
                if (!func.apply(clusterName)) {
                    continue;
                }

                String clusterPropJson = getZkClient().readData(
                        clusterRootPath + File.separator + clusterName, true);
                if (StringUtils.isNotBlank(clusterPropJson)) {
                    Cluster cluster = JsonUtils.toT(clusterPropJson,
                            Cluster.class);
                    clusterList.add(cluster);

                    if (initNodes) {
                        initNodes(clusterName, cluster);
                    }
                }

                if (limit > 0 && ++found >= limit) {
                    break;
                }
            }
        }
        return clusterList;
    }

    @Override
    public List<String> getClusterNamesMatch(Function<String, Boolean> func) {
        List<String> allClusterNames = getZkClient().getChildren(clusterRootPath);
        if (CollectionUtils.isEmpty(allClusterNames)) {
            return Collections.emptyList();
        }
        List<String> clusterNames = new ArrayList<>();
        for (String clusterName : allClusterNames) {
            if (BooleanUtils.isTrue(func.apply(clusterName))) {
                clusterNames.add(clusterName);
            }
        }
        return clusterNames;
    }

    public List<Cluster> getClustersByNamePrefix(final String pattern, int limit) {
        Function<String, Boolean> func = input -> StringUtils.startsWith(input, pattern);

        return myFindClustersByNamePrefix(limit, func, true);
    }

    private void initNodes(String clusterName, Cluster cluster) {
        String clusterPath = clusterRootPath + File.separator + clusterName;
        List<String> nodeNames = getZkClient().getChildren(clusterPath);
        for (String nodeName : nodeNames) {
            nodeName = stripName(nodeName);
            Node node = cluster.findNodeByName(nodeName);
            if (node != null) {
                continue;
            }
            String nodePath = clusterPath + File.separator + nodeName;
            String nodePropJson = getZkClient().readData(nodePath, true);
            node = JsonUtils.toT(nodePropJson, Node.class);

            initInstances(nodePath, node);

            cluster.addNode(node);
        }
    }

    private void initInstances(String nodePath, Node node) {
        List<String> instanceNames = getZkClient().getChildren(nodePath);
        for (String instanceName : instanceNames) {
            instanceName = stripName(instanceName);
            Instance instance = NodeUtils.findInstanceByName(node, instanceName);
            if (instance != null) {
                continue;
            }
            String instancePath = nodePath + File.separator + instanceName;
            String instancePropJson = getZkClient()
                    .readData(instancePath, true);
            instance = JsonUtils.toT(instancePropJson, Instance.class);

            node.addInstance(instance);
        }
    }

    @Override
    public List<Cluster> getClusterByPrefix(String pattern) {
        return findClustersByNamePrefix(pattern, -1);
    }

    @Override
    public List<Instance> getAllInstancesByName(String clusterName) {
        List<Instance> instanceList = null;
        List<Cluster> clusters = findClustersByNamePrefix(clusterName, 1);
        for (Cluster cluster : clusters) {
            instanceList = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(cluster.getNodeList())) {
                for (Node node : cluster.getNodeList()) {
                    if (CollectionUtils.isNotEmpty(node.getInstances())) {
                        instanceList.addAll(node.getInstances());
                    }
                }
            }
        }
        return instanceList;
    }

    @Override
    public synchronized boolean saveOrUpdateInstance(String clusterName,
                                                     Instance instance) {
        if (instance == null) {
            return false;
        }
        String nodeName = instance.getNodeName();
        String instanceName = instance.getName();
        String instancePropJson = JsonUtils.toJSON(instance);
        if (StringUtils.isAnyBlank(clusterName, nodeName, instanceName,
                instancePropJson)) {
            return false;
        }

        // update old instance or create a new one
        String clusterPath = clusterRootPath + File.separator + clusterName;
        List<String> nodeList = getZkClient().getChildren(clusterPath);
        if (CollectionUtils.isNotEmpty(nodeList)) {
            for (String node : nodeList) {
                if (node.equalsIgnoreCase(nodeName)) {
                    String nodePath = clusterPath + File.separator + nodeName;
                    String instancePath = nodePath + File.separator + instanceName;

                    List<String> instances = getZkClient().getChildren(nodePath);
                    if (CollectionUtils.isNotEmpty(instances)) {
                        for (String _instance : instances) {
                            if (_instance.equalsIgnoreCase(instanceName)) {
                                getZkClient().writeData(instancePath,
                                        instancePropJson);
                                return true;
                            }
                        }
                    }
                    getZkClient().createPersistent(instancePath,
                            instancePropJson);
                    return true;
                }
            }
        }

        // add new node
        Node node = new Node();
        node.setClusterName(clusterName);
        node.setName(nodeName);
        Date now = new Date();
        node.setAddTime(now);
        node.setLastModifyTime(now);
        List<Instance> instancesList = new ArrayList<>(1);
        instancesList.add(instance);
        node.setInstances(instancesList);
        String nodePath = clusterRootPath + File.separator + clusterName + File.separator + nodeName;
        getZkClient().createPersistent(nodePath, JsonUtils.toJSON(node));
        // add new instance
        String instancePath = nodePath + File.separator + instanceName;
        getZkClient().createPersistent(instancePath, instancePropJson);
        return true;
    }

    @Override
    public synchronized boolean saveOrUpdateCluster(Cluster cluster) {
        if (cluster == null || StringUtils.isBlank(cluster.getName())) {
            return false;
        }
        cluster.setClusterType(clusterType);
        String clusterPropJson = JsonUtils.toJSON(cluster);
        if (StringUtils.isBlank(clusterPropJson)) {
            return false;
        }

        String clusterPath = clusterRootPath + File.separator + cluster.getName();
        if (getZkClient().exists(clusterPath)) {
            getZkClient().writeData(clusterPath, clusterPropJson);
        } else {
            getZkClient().createPersistent(clusterPath, clusterPropJson);
        }
        return true;
    }

    @Override
    public synchronized boolean saveOrUpdateNode(String clusterName, Node node) {
        if (node == null) {
            return false;
        }
        String nodeName = node.getName();
        String nodePropJson = JsonUtils.toJSON(node);
        if (StringUtils.isAnyBlank(clusterName, nodeName, nodePropJson)) {
            return false;
        }

        String clusterPath = clusterRootPath + File.separator + clusterName;
        if (!getZkClient().exists(clusterPath)) {
            getZkClient().createPersistent(clusterPath,
                    JsonUtils.toJSON(new Cluster()));
        }
        String nodePath = clusterPath + File.separator + nodeName;
        List<String> nodeList = getZkClient().getChildren(clusterPath);
        if (CollectionUtils.isNotEmpty(nodeList)) {
            for (String _node : nodeList) {
                if (_node.equalsIgnoreCase(nodeName)) {
                    getZkClient().writeData(nodePath, nodePropJson);
                    return true;
                }
            }
        }
        getZkClient().createPersistent(nodePath, nodePropJson);
        return true;
    }

    @Override
    public synchronized boolean deleteInstance(String clusterName,
                                               String nodeName, String instanceName) {
        if (StringUtils.isAnyBlank(clusterName, nodeName, instanceName)) {
            return false;
        }

        String clusterPath = clusterRootPath + File.separator + clusterName;
        List<String> nodeList = getNodeList(clusterPath);
        if (CollectionUtils.isNotEmpty(nodeList)) {
            for (String node : nodeList) {
                if (node.equalsIgnoreCase(nodeName)) {
                    String nodePath = clusterPath + File.separator + node;
                    List<String> instances = getZkClient()
                            .getChildren(nodePath);
                    if (CollectionUtils.isNotEmpty(instances)) {
                        for (String instance : instances) {
                            if (instance.equalsIgnoreCase(instanceName)) {
                                String instancePath = nodePath + File.separator
                                        + instanceName;
                                getZkClient().delete(instancePath);
                                return true;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    private List<String> getNodeList(String clusterPath) {
        List<String> nodeList = null;
        try {
            nodeList = getZkClient().getChildren(clusterPath);
        } catch (ZkNoNodeException e) {
            logger.error(e.getMessage());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return nodeList;
    }

    @Override
    public synchronized boolean deleteNode(String clusterName, String nodeName) {
        if (StringUtils.isAnyBlank(clusterName, nodeName)) {
            return false;
        }

        String clusterPath = clusterRootPath + File.separator + clusterName;
        List<String> nodeList = getNodeList(clusterPath);
        if (CollectionUtils.isNotEmpty(nodeList)) {
            for (String node : nodeList) {
                if (node.equalsIgnoreCase(nodeName)) {
                    String nodePath = clusterPath + File.separator + nodeName;
                    getZkClient().deleteRecursive(nodePath);
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Node findNodeByName(String clusterName, String nodeName) {
        if (StringUtils.isAnyBlank(clusterName, nodeName)) {
            return null;
        }

        String clusterPath = clusterRootPath + File.separator + clusterName;
        List<String> nodeNames = getZkClient().getChildren(clusterPath);
        if (CollectionUtils.isNotEmpty(nodeNames)) {
            for (String _nodeName : nodeNames) {
                if (_nodeName.equalsIgnoreCase(nodeName)) {
                    String nodePath = clusterPath + File.separator + nodeName;
                    String nodePropJson = getZkClient()
                            .readData(nodePath, true);
                    Node node = JsonUtils.toT(nodePropJson, Node.class);
                    initInstances(nodePath, node);
                    return node;
                }
            }
        }
        return null;
    }

    @Override
    public Instance findInstanceByName(String clusterName, String nodeName,
                                       String instanceName) {
        if (StringUtils.isAnyBlank(clusterName, nodeName, instanceName)) {
            return null;
        }

        String clusterPath = clusterRootPath + File.separator + clusterName;
        List<String> nodeNames = getZkClient().getChildren(clusterPath);
        if (CollectionUtils.isNotEmpty(nodeNames)) {
            for (String _nodeName : nodeNames) {
                if (_nodeName.equalsIgnoreCase(nodeName)) {
                    String nodePath = clusterPath + File.separator + nodeName;
                    List<String> instanceNames = getZkClient().getChildren(
                            nodePath);
                    if (CollectionUtils.isEmpty(instanceNames)) {
                        return null;
                    }
                    for (String _instanceName : instanceNames) {
                        if (_instanceName.equalsIgnoreCase(instanceName)) {
                            String instancePath = nodePath + File.separator + instanceName;
                            String instancePropJson = getZkClient().readData(
                                    instancePath, true);
                            return JsonUtils.toT(instancePropJson,
                                    Instance.class);
                        }
                    }
                }
            }
        }
        return null;
    }

    @Override
    public List<Node> findNodeByClusterName(String clusterName) {
        if (StringUtils.isAnyBlank(clusterName)) {
            return Lists.newArrayList();
        }
        List<Node> nodes = new ArrayList<>();
        String clusterPath = clusterRootPath + File.separator + clusterName;
        List<String> nodeNames = getZkClient().getChildren(clusterPath);
        if (CollectionUtils.isNotEmpty(nodeNames)) {
            for (String _nodeName : nodeNames) {
                String nodePath = clusterPath + File.separator + _nodeName;
                String nodePropJson = getZkClient().readData(nodePath, true);
                Node node = JsonUtils.toT(nodePropJson, Node.class);
                initInstances(nodePath, node);
                nodes.add(node);

            }
        }
        return nodes;
    }

    @Override
    public List<Instance> findInstanceByNodeName(String clusterName,
                                                 String nodeName) {
        if (StringUtils.isAnyBlank(clusterName, nodeName)) {
            return Lists.newArrayList();
        }

        List<Instance> instances = new ArrayList<>();

        String clusterPath = clusterRootPath + File.separator + clusterName;

        List<String> nodeNames = getZkClient().getChildren(clusterPath);
        if (CollectionUtils.isNotEmpty(nodeNames)) {
            for (String _nodeName : nodeNames) {
                if (_nodeName.equalsIgnoreCase(nodeName)) {
                    String nodePath = clusterPath + File.separator + nodeName;
                    List<String> instanceNames = getZkClient().getChildren(
                            nodePath);
                    if (CollectionUtils.isEmpty(instanceNames)) {
                        return Lists.newArrayList();
                    }
                    for (String _instanceName : instanceNames) {
                        String instancePath = nodePath + File.separator + _instanceName;
                        String instancePropJson = getZkClient().readData(
                                instancePath, true);
                        instances.add(JsonUtils.toT(instancePropJson,
                                Instance.class));
                    }
                }
            }
        }
        return instances;
    }

    @Override
    public boolean deleteCluster(String clusterName) {
        logger.info("start to delete the cluster:" + clusterName);

        ClusterStatusManager stm = ClusterStatusManager.getInstance();
        Map<String, ReHashStatus> clientStatuses = stm.getClientStatuses(clusterName);
        if (clientStatuses != null && clientStatuses.size() > 0) {
            throw new RuntimeException("Not allowed to delete the cluster :" + clusterName + " has clients" + JsonUtils.toJSON(clientStatuses));
        }

        String clusterPath = clusterRootPath + File.separator + clusterName;
        return getZkClient().deleteRecursive(clusterPath);
    }


    @Override
    public void addClusterToMonitor(String cluster, ClusterType clusterType) {
        Validate.isTrue(clusterType == REDIS, "clusterType only support redis in monitor mode");
        ZkClient zkClient = getZkClient();
        logger.info("start to add cluster " + cluster + " to be monitored");
        if (!zkClient.exists(MONITOR_PATH))
            zkClient.createPersistent(MONITOR_PATH);
        if (!zkClient.exists(MONITOR_PATH + File.separator + cluster))
            zkClient.createPersistent(MONITOR_PATH + File.separator + cluster);
        if (!zkClient.exists(MONITOR_PATH + File.separator + cluster + File.separator + TYPE))
            zkClient.createPersistent(MONITOR_PATH + File.separator + cluster + File.separator + TYPE, REDIS);
    }

}
