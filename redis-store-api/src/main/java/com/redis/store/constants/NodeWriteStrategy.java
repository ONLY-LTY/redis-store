package com.redis.store.constants;

public enum NodeWriteStrategy {
    MASTER("master"), MUILTI_MASTER("multi_master"), RANDOM("random");

    private String value;

    NodeWriteStrategy(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public static NodeWriteStrategy valueof(String value) {
        for (NodeWriteStrategy v : values()) {
            if (v.value.equalsIgnoreCase(value))
                return v;
        }
        throw new IllegalArgumentException();
    }
}
