package com.redis.store.listener;


public interface NodeAliveListener {

    void onAlive(String nodeName);

    void onDead(String nodeName);
}
