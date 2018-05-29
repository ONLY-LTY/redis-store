package com.redis.store.listener;


import com.redis.store.cluster.Instance;

public interface InstanceChangeListener {
    public boolean instanceDataChanged(Instance newInstance, String nodeName);

    public boolean instanceAdded(Instance newInstance, String nodeName);

    public boolean instanceDeleted(String instanceName, String nodeName);
}
