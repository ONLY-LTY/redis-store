package com.redis.store.constants;

public enum ReHashStatus {
    NORMAL("normal"), SYN("syn"), ACK("ack"), FAIL("fail"), REHASH("rehash"), FINISHED(
            "finished");

    private String value;

    ReHashStatus(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public static ReHashStatus valueof(String value) {
        for (ReHashStatus v : values()) {
            if (v.value.equalsIgnoreCase(value))
                return v;
        }
        throw new IllegalArgumentException();
    }
}
