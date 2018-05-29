package com.redis.store.proxy;

public interface RedisInvocationListener {

    RedisInvocationListener EMPTY = targetHostPort -> {
        // do nothing
    };

    void onComplete(String targetHostPort);
}
