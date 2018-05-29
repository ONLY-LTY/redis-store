package com.redis.store.constants;

public enum NodeStatus {
    ACTIVE("active"), DISABLE("disable"), CLOSED("closed");

    private String value;

    NodeStatus(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public static NodeStatus valueof(String value) {
        for (NodeStatus v : values()) {
            if (v.value.equalsIgnoreCase(value))
                return v;
        }
        throw new IllegalArgumentException();
    }
}
