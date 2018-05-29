package com.redis.store.cluster;

import com.redis.store.constants.ActiveStatus;
import com.redis.store.constants.MasterSlaveStatus;
import com.redis.store.constants.ReplicationState;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown=true) 
public class Instance implements Serializable {

    private static final long serialVersionUID = 4587618177201105726L;
    private String nodeName;
    private String name;
    private String domain;
    private Date addTime;
    private Date lastModifyTime;
    private String hostName;
    private Integer port;
    private ActiveStatus status;
    private MasterSlaveStatus msStatus;
    private ReplicationState replicationState = ReplicationState.INITIAL;
    private int priority;

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

    @Override
    public int hashCode() {
        int result = nodeName != null ? nodeName.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (addTime != null ? addTime.hashCode() : 0);
        result = 31 * result
                + (lastModifyTime != null ? lastModifyTime.hashCode() : 0);
        result = 31 * result + (status != null ? status.hashCode() : 0);
        result = 31 * result + (msStatus != null ? msStatus.hashCode() : 0);
        result = 31 * result + (domain != null ? domain.hashCode() : 0);
        result = 31 * result + (hostName != null ? hostName.hashCode() : 0);
        result = 31 * result + (port != null ? port.hashCode() : 0);
        result = 31 * result + (replicationState != null ? replicationState.hashCode() : 0);
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

        Instance instance = (Instance) obj;
        if (this.getName() != null) {
            if (!this.getName().equals(instance.getName()))
                return false;
        } else {
            if (instance.getName() != null)
                return false;
        }

        if (this.getNodeName() != null) {
            if (!this.getNodeName().equals(instance.getNodeName()))
                return false;
        } else {
            if (instance.getNodeName() != null)
                return false;
        }

        if (this.getDomain() != null) {
            if (!this.getDomain().equals(instance.getDomain()))
                return false;
        } else {
            if (instance.getDomain() != null)
                return false;
        }

        if (this.getHostName() != null) {
            if (!this.getHostName().equals(instance.getHostName()))
                return false;
        } else {
            if (instance.getHostName() != null)
                return false;
        }

        if (this.getPort() != null) {
            if (!this.getPort().equals(instance.getPort()))
                return false;
        } else {
            if (instance.getPort() != null)
                return false;
        }

        if (this.getAddTime() != null) {
            if (!this.getAddTime().equals(instance.getAddTime()))
                return false;
        } else {
            if (instance.getAddTime() != null)
                return false;
        }

        if (this.getStatus() != null) {
            if (!this.getStatus().equals(instance.getStatus()))
                return false;
        } else {
            if (instance.getStatus() != null)
                return false;
        }

        if (this.getMsStatus() != null) {
            if (!this.getMsStatus().equals(instance.getMsStatus()))
                return false;
        } else {
            if (instance.getMsStatus() != null)
                return false;
        }

        if (this.getLastModifyTime() != null) {
            if (!this.getLastModifyTime().equals(instance.getLastModifyTime()))
                return false;
        } else {
            if (instance.getLastModifyTime() != null)
                return false;
        }

        return true;

    }

    @Override
    public String toString() {
        return "instance[name:" + name + " domain:" + domain + " nodeName:"
                + nodeName + " lastModifyTime:" + lastModifyTime + " status:"
                + status + " msStatus:" + msStatus + " replicationState:" +replicationState+ "]";
    }

    public ActiveStatus getStatus() {
        return status;
    }

    public void setStatus(ActiveStatus status) {
        this.status = status;
    }

    public MasterSlaveStatus getMsStatus() {
        return msStatus;
    }

    public void setMsStatus(MasterSlaveStatus msStatus) {
        this.msStatus = msStatus;
    }

    public ReplicationState getReplicationState() {
        return replicationState;
    }

    public void setReplicationState(ReplicationState replicationState) {
        this.replicationState = replicationState;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public boolean checkInstanceActive() {
        return status != ActiveStatus.DELETED;
    }

    public boolean checkMaster() {
        return msStatus == MasterSlaveStatus.MASTER
                || msStatus == MasterSlaveStatus.ALL;
    }

    public boolean checkSlave() {
        return msStatus == MasterSlaveStatus.SLAVE
                || msStatus == MasterSlaveStatus.ALL;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String findHostPort() {
        return this.getDomain() + ":" + this.getPort();
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }
    

}
