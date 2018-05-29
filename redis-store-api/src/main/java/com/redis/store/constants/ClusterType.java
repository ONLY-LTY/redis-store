package com.redis.store.constants;

public enum ClusterType {

    REDIS("/clusters"),

    MONGODB("/mongoclusters"),

    MYSQL("/mysqlclusters");

    private final String configRootPath;

    ClusterType(String configRootPath) {
        this.configRootPath = configRootPath;
    }

    public String getConfigRootPath() {
        return configRootPath;
    }
}
