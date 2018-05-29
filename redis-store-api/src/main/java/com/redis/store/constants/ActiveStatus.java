package com.redis.store.constants;

public enum ActiveStatus {
    ACTIVE("active"), DOWN("down"), DELETED("deleted");
    private String value;

    ActiveStatus(String value) {
        this.value = value;
    }

    public static ActiveStatus valueof(String value) {
        for (ActiveStatus v : values()) {
            if (v.value.equalsIgnoreCase(value))
                return v;
        }
        throw new IllegalArgumentException();
    }

    @Override
    public String toString() {
        return this.value;
    }
}
