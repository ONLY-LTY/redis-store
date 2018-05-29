package com.redis.store.constants;

public enum ClusterShardStrategy {

    MOD,

    CONSISTENT_HASH,

    REGION,

    REGION_SUFFIX,

    PROXY,

    ONESTORE_PROXY,

    HEX_PREFIX_MOD;

    @Override
    public String toString() {
        return this.name();
    }

    public static ClusterShardStrategy valueof(String value) {
        for (ClusterShardStrategy v : values()) {
            if (v.name().equalsIgnoreCase(value))
                return v;
        }
        throw new IllegalArgumentException();
    }
    
}
