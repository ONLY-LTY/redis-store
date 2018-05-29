package com.redis.store.events;


public abstract class ClusterEvent extends StoreEvent {

    public ClusterEvent() {
        super(Type.CLUSTER);
    }
}
