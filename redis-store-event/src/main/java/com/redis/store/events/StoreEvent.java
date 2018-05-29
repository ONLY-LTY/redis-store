package com.redis.store.events;

import java.util.List;


public abstract class StoreEvent {

    enum Type {
        CLUSTER, NODE, INSTANCE
    }

    final Type type;

    protected StoreEvent(Type type) {
        this.type = type;
    }

    public abstract <T> void notify(List<T> listeners);
}
