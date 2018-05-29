package com.redis.store.util;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ZookeeperUtils {

    private static volatile CuratorFramework zkClient = null;

    public static CuratorFramework getZookeeperClient() {
        if (zkClient == null) {
            synchronized (ZookeeperUtils.class) {
                if (zkClient == null) {
                    String zkServers = ZkClientUtils.getZkServers();
                    CuratorFramework client = CuratorFrameworkFactory.builder()
                            .connectString(zkServers)
                            .retryPolicy(new ExponentialBackoffRetry(1000, 16, 60000))
                            .build();
                    client.start();
                    try {
                        if (!client.blockUntilConnected(8, SECONDS)) {
                            throw new RuntimeException("Cannot connect to Zookeeper: " + zkServers);
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    zkClient = client;
                }
            }
        }
        return zkClient;
    }
}
