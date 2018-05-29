package com.redis.store.constants;

public enum MasterSlaveStatus {
    MASTER("master"), SLAVE("slave"), ALL("all");

    private String value;

    MasterSlaveStatus(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public static MasterSlaveStatus valueof(String value) {
        for (MasterSlaveStatus v : values()) {
            if (v.value.equalsIgnoreCase(value))
                return v;
        }
        throw new IllegalArgumentException();
    }
}
