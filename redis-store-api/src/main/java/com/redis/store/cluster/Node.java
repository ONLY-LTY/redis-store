package com.redis.store.cluster;

import com.redis.store.constants.NodeReadStrategy;
import com.redis.store.constants.NodeStatus;
import com.redis.store.constants.NodeWriteStrategy;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Node implements Serializable {

    private static final long serialVersionUID = -4849086864395712862L;
    private String clusterName;
    private String name;
    private Date addTime;
    private Date lastModifyTime;
    private List<Instance> instances = new CopyOnWriteArrayList<>();
    private NodeWriteStrategy writeStrategy = NodeWriteStrategy.MASTER;
    private NodeReadStrategy readStrategy = NodeReadStrategy.SLAVES;
    private NodeStatus status = NodeStatus.ACTIVE;
    private int priority;

    private long start = 0;
    private long end = 0;

    private long secondShardKey = 0;

    private long warmupBeginTime;

    public Node() {
        // For jackson.
    }

    public long getWarmupBeginTime() {
        return warmupBeginTime;
    }

    public void setWarmupBeginTime(long warmupBeginTime) {
        this.warmupBeginTime = warmupBeginTime;
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
    public List<Instance> getInstances() {
        return instances;
    }

    @JsonIgnore
    public void setInstances(List<Instance> instances) {
        this.instances = instances;
    }


    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + clusterName != null ? clusterName.hashCode() : 0;
        result = 31 * result + (addTime != null ? addTime.hashCode() : 0);
        result = 31 * result
                + (lastModifyTime != null ? lastModifyTime.hashCode() : 0);
        if (!CollectionUtils.isEmpty(instances)) {
            for (Instance instance : instances) {
                result = 31 * result + instance.hashCode();
            }
        }
        result = 31 * result + (status != null ? status.hashCode() : 0);
        result = 31 * result
                + (writeStrategy != null ? writeStrategy.hashCode() : 0);
        result = 31 * result
                + (readStrategy != null ? readStrategy.hashCode() : 0);
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

        Node node = (Node) obj;
        if (this.getName() != null) {
            if (!this.getName().equals(node.getName()))
                return false;
        } else {
            if (node.getName() != null)
                return false;
        }

        if (this.getClusterName() != null) {
            if (!this.getClusterName().equals(node.getClusterName()))
                return false;
        } else {
            if (node.getClusterName() != null)
                return false;
        }

        if (this.getAddTime() != null) {
            if (!this.getAddTime().equals(node.getAddTime()))
                return false;
        } else {
            if (node.getAddTime() != null)
                return false;
        }

        if (this.getStatus() != null) {
            if (!this.getStatus().equals(node.getStatus()))
                return false;
        } else {
            if (node.getStatus() != null)
                return false;
        }

        if (this.getLastModifyTime() != null) {
            if (!this.getLastModifyTime().equals(node.getLastModifyTime()))
                return false;
        } else {
            if (node.getLastModifyTime() != null)
                return false;
        }

        if (this.getInstances() != null) {
            if (!this.getInstances().equals(node.getInstances()))
                return false;
        } else {
            if (node.getInstances() != null)
                return false;
        }

        if (this.getReadStrategy() != null) {
            if (!this.getReadStrategy().equals(node.getReadStrategy()))
                return false;
        } else {
            if (node.getReadStrategy() != null)
                return false;
        }

        if (this.getWriteStrategy() != null) {
            if (!this.getWriteStrategy().equals(node.getWriteStrategy()))
                return false;
        } else {
            if (node.getWriteStrategy() != null)
                return false;
        }

        return true;

    }

    @Override
    public String toString() {
        return "node[name:" + name + " clusterName:" + clusterName
                + " addTime:" + addTime + " lastModifyTime:" + lastModifyTime
                + " writeStrategy:" + writeStrategy + " readStrategy:"
                + readStrategy + "status:" + status + ", instances:"
                + ArrayUtils.toString(instances.toArray(new Instance[]{}))
                + "]";
    }

    public NodeWriteStrategy getWriteStrategy() {
        return writeStrategy;
    }

    public void setWriteStrategy(NodeWriteStrategy writeStrategy) {
        this.writeStrategy = writeStrategy;
    }

    public NodeReadStrategy getReadStrategy() {
        return readStrategy;
    }

    public void setReadStrategy(NodeReadStrategy readStrategy) {
        this.readStrategy = readStrategy;
    }

    public NodeStatus getStatus() {
        return status;
    }

    public void setStatus(NodeStatus status) {
        this.status = status;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public void addInstance(Instance instance) {
        instances.add(instance);
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public long getSecondShardKey() {
        return secondShardKey;
    }

    public void setSecondShardKey(long secondShardKey) {
        this.secondShardKey = secondShardKey;
    }
}
