package com.redis.store.events;

public abstract class InstanceEvent extends StoreEvent {

    public InstanceEvent() {
        super(Type.INSTANCE);
    }
}
