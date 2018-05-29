package com.redis.store.constants;

public enum NodeReadStrategy {
    MASTER("master"), SLAVES("slaves"), RANDOM("random");
    
    private String value;

    NodeReadStrategy(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public static NodeReadStrategy valueof(String value) {
        for (NodeReadStrategy v : values()) {
            if (v.value.equalsIgnoreCase(value))
                return v;
        }
        throw new IllegalArgumentException();
    }
}
