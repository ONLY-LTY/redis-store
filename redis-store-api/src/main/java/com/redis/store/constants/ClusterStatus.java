package com.redis.store.constants;

public enum ClusterStatus {
    NORMAL("nomal"), REHASH("rehash"), DISABLE("disable");
    
    private String value;

    ClusterStatus(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return this.value;
    }

    public static ClusterStatus valueof(String value) {
        for (ClusterStatus v : values()) {
            if (v.value.equalsIgnoreCase(value))
                return v;
        }
        throw new IllegalArgumentException();
    }
}
