package com.redis.store.cluster.factory;

import com.google.common.base.Function;
import com.redis.store.cluster.Cluster;
import com.redis.store.cluster.Instance;
import com.redis.store.cluster.Node;
import com.redis.store.constants.ClusterType;

import java.util.List;

public interface IClusterManager {
    /**
     * @param pattern 集群前缀
     * @return List<Cluster> 所有集群列表
     * */
    public List<Cluster> getClusterByPrefix(String pattern);

    /**
     * 通过集群名获取集群整体信息
     *
     * @param clusterName     集群名
     * @param isFetchingNodes 是否一并获取该集群的所有节点(包含实例)信息
     * @return 集群
     */
    Cluster findClusterByName(String clusterName, boolean isFetchingNodes);

    /**
     * 根据集群名称前缀返回集群集合(正则)
     * @param pattern
     * @param limit
     * @return
     */
    public List<Cluster> findClustersByNamePrefix(String pattern, int limit);

    List<Cluster> getMatchingClusters(String pattern, boolean initNodes);

    List<String> getClusterNamesMatch(Function<String, Boolean> func);

    /**
     * 根据集群名称前缀返回集群集合
     * @param pattern
     * @param limit   返回的数量
     * @return
     */
    public List<Cluster> getClustersByNamePrefix(String pattern, int limit);
    
    /**
     * @param clusterName 集群名称
     * @return List<Instance> 集群下所有instance 列表
     * */
    public List<Instance> getAllInstancesByName(String clusterName);
    
    /**
     * @param clusterName 集群名称
     * @param instance 实例对象
     * */
    public boolean saveOrUpdateInstance(String clusterName, Instance instance);
    
    /**
     * @param cluster 集群对象
     * 只对集群对象进行存储 不涉及到Node instance 的修改
     * */
    public boolean saveOrUpdateCluster(Cluster cluster);
    
    /**
     * @param clusterName 集群名称
     * @param Node node 节点对象
     * 只对集群下相应的节点对象进行存储 不涉及到instance 的修改
     * */
    public boolean saveOrUpdateNode(String clusterName, Node node);
    
    /**
     * @param clusterName 集群名称
     * @param nodeName 节点名称
     * @param instanceName 实例名称
     * 删除cluster下的某个instance
     * */
    public boolean deleteInstance(String clusterName, String nodeName,
                                  String instanceName);
    
    /**
     * @param clusterName 集群名称
     * @param nodeName 节点名称
     * 删除cluster下的某个node
     * */
    public boolean deleteNode(String clusterName, String nodeName);
    
    /**
     * @param clusterName 集群名称
     * @param nodeName 节点名称
     * @return Node 对象
     * */
    public Node findNodeByName(String clusterName, String nodeName);
    
    /**
     * @param clusterName 集群名称
     * @param nodeName 节点名称
     * @param instanceName 实例名称
     * @return Instance 实例对象
     * */
    public Instance findInstanceByName(String clusterName, String nodeName, String instanceName);
    
    /**
     * @param clusterName 集群名称
     * @return List<Node> 对象
     * */
    public List<Node> findNodeByClusterName(String clusterName);
    
    /**
     * @param clusterName 集群名称
     * @return List<Node> 对象
     * */
    public List<Instance> findInstanceByNodeName(String clusterName, String nodeName);
    
    
    /**
     * @param clusterName 集群名称
     * 通过clusterName 删除cluster
     * @return boolean 是否删除成功
     * */
    public boolean deleteCluster(String clusterName);

    void addClusterToMonitor(String cluster, ClusterType clusterType);

}
