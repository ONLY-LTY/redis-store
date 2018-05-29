package com.redis.store.exception;


import com.redis.store.constants.ClusterType;


public class NoClusterOrNodeException extends Exception {

    public NoClusterOrNodeException(ClusterType clusterType, String clusterOrNodeName) {
        super(clusterType + " cluster or node " + clusterOrNodeName + " is not defined!");
    }
}
