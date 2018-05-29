package com.redis.store.listener;


import com.redis.store.cluster.Node;

public interface NodeChangedListener {
    public boolean nodeAdd(Node newNode);

    public boolean nodeDeleted(String nodeName);

    public boolean nodeDataChanged(Node newNode);
}
