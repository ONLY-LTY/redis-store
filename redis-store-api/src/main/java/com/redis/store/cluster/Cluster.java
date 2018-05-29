package com.redis.store.cluster;

import com.redis.store.constants.ClusterShardStrategy;
import com.redis.store.constants.ClusterStatus;
import com.redis.store.constants.ClusterType;
import com.redis.store.constants.NodeStatus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Cluster implements Serializable {

    private static final long serialVersionUID = -8329247859682403166L;
    private String name;
    private Date addTime;
    private Date lastModifyTime;
    private List<Node> nodeList = new CopyOnWriteArrayList<>();
    private ClusterStatus status;
    private ClusterShardStrategy shardStrategy = ClusterShardStrategy.CONSISTENT_HASH;
    private ClusterType clusterType = ClusterType.REDIS;

    private volatile int nodeWarmupDurationSec;

    public int getNodeWarmupDurationSec() {
        return nodeWarmupDurationSec;
    }

    public void setNodeWarmupDurationSec(int nodeWarmupDurationSec) {
        this.nodeWarmupDurationSec = nodeWarmupDurationSec;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getAddTime() {
        return addTime;
    }

    public void setAddTime(Date addTime) {
        this.addTime = addTime;
    }

    public Date getLastModifyTime() {
        return lastModifyTime;
    }

    public void setLastModifyTime(Date lastModifyTime) {
        this.lastModifyTime = lastModifyTime;
    }

    @JsonIgnore
    public List<Node> getNodeList() {
        return nodeList;
    }

    @JsonIgnore
    public List<Node> getActiveNodes() {
        List<Node> activeNodes = new ArrayList<>();
        for (Node node : nodeList) {
            if (node.getStatus() != NodeStatus.ACTIVE)
                continue;
            activeNodes.add(node);
        }
        return activeNodes;
    }

    @JsonIgnore
    public void setNodeList(List<Node> nodeList) {
        this.nodeList = nodeList;
    }

    public void addNode(Node node) {
        nodeList.add(node);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (addTime != null ? addTime.hashCode() : 0);
        result = 31 * result
                + (lastModifyTime != null ? lastModifyTime.hashCode() : 0);
        if (!CollectionUtils.isEmpty(nodeList)) {
            for (Node node : nodeList) {
                result = 31 * result + node.hashCode();
            }
        }
        result = 31 * result + (status != null ? status.hashCode() : 0);
        return result;

    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        Cluster cluster = (Cluster) obj;
        if (this.getName() != null) {
            if (!this.getName().equals(cluster.getName()))
                return false;
        } else {
            if (cluster.getName() != null)
                return false;
        }

        if (this.getAddTime() != null) {
            if (!this.getAddTime().equals(cluster.getAddTime()))
                return false;
        } else {
            if (cluster.getAddTime() != null)
                return false;
        }

        if (this.getStatus() != null) {
            if (!this.getStatus().equals(cluster.getStatus()))
                return false;
        } else {
            if (cluster.getStatus() != null)
                return false;
        }

        if (this.getLastModifyTime() != null) {
            if (!this.getLastModifyTime().equals(cluster.getLastModifyTime()))
                return false;
        } else {
            if (cluster.getLastModifyTime() != null)
                return false;
        }

        if (this.getNodeList() != null) {
            if (!this.getNodeList().equals(cluster.getNodeList()))
                return false;
        } else {
            if (cluster.getNodeList() != null)
                return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "cluster[name:" + name + ",addTime:" + addTime
                + ", lastModifyTime:" + lastModifyTime + ", status:" + status
                + ", nodeList:"
                + ArrayUtils.toString(nodeList.toArray(new Node[]{})) + " ]";
    }

    public ClusterStatus getStatus() {
        return status;
    }

    public void setStatus(ClusterStatus status) {
        this.status = status;
    }

    public Node findNodeByName(String name) {
        for (Node node : nodeList) {
            if (name.equals(node.getName())) {
                return node;
            }
        }
        return null;
    }

    public Node removeNodeByName(String nodeName) {
        //CopyOnWriteArrayList不能Iterator remove
        for (Node node : nodeList) {
            if (node.getName().equals(nodeName)) {
                node.setStatus(NodeStatus.CLOSED);
                nodeList.remove(node);
                return node;
            }
        }
        return null;
    }

    public ClusterShardStrategy getShardStrategy() {
        return shardStrategy;
    }

    public void setShardStrategy(ClusterShardStrategy shardStrategy) {
        this.shardStrategy = shardStrategy;
    }

    public ClusterType getClusterType() {
        return clusterType;
    }

    public void setClusterType(ClusterType clusterType) {
        this.clusterType = clusterType;
    }

}
