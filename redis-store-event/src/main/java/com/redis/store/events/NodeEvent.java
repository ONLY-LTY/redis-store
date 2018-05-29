package com.redis.store.events;


public abstract class NodeEvent extends StoreEvent {

    public NodeEvent() {
        super(Type.NODE);
    }
}
