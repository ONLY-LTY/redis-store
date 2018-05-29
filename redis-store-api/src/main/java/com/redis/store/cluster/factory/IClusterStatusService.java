package com.redis.store.cluster.factory;

import com.redis.store.constants.ClusterType;
import com.redis.store.constants.ReHashStatus;

import java.util.Map;

public interface IClusterStatusService {
    /**
     * @param clusterName 集群名称
     * @return 当前集群的rehash 状态
     * */
    @Deprecated
    public ReHashStatus getClusterStatus(String clusterName);
    
    /**
     * @param clusterName 集群名称
     * @param 集群的rehash 状态
     * */
    @Deprecated
    public void updateClusterStatus(String clusterName, ReHashStatus status);
    
    /**
     * 获取集群下所有client的rehash 状态
     * @param clusterName 集群名称
     * @return Map<String, ReHashStatus> key:客户端名称 value:客户端rehash 状态
     * */
    @Deprecated
    public Map<String, ReHashStatus> getClientStatuses(String clusterName);
    
    /**
     * 更改某个客户端的rehash 状态
     * @param clusterName 集群名称
     * @param clientName  客户端名称
     * @param 集群的rehash 状态
     * */
    @Deprecated
    public void  updateClientStatus(String clusterName, String clientName, ReHashStatus status);

    ReHashStatus getClusterStatus(ClusterType clusterType, String clusterName);

    void updateClusterStatus(ClusterType clusterType, String clusterName, ReHashStatus status);

    Map<String, ReHashStatus> getClientStatuses(ClusterType clusterType, String clusterName);

    void updateClientStatus(ClusterType clusterType, String clusterName, String clientName, ReHashStatus status);
}
