package com.redis.store.constants;

public enum ReplicationState {
    NONEED("noneed"), SYNCING("syncing"), SYNCED("synced"),INITIAL("initial");

    private String value;

    ReplicationState(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public static ReplicationState valueof(String value) {
        for (ReplicationState v : values()) {
            if (v.value.equalsIgnoreCase(value))
                return v;
        }
        throw new IllegalArgumentException();
    }
}
