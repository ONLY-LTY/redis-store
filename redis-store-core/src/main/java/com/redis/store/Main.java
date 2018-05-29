package com.redis.store;

import com.redis.store.cluster.Cluster;
import com.redis.store.cluster.Instance;
import com.redis.store.cluster.Node;
import com.redis.store.constants.*;
import com.redis.store.proxy.StoreDaoFactory;
import com.redis.store.service.RegistryService;
import com.redis.store.util.JsonUtils;
import com.redis.store.util.ZookeeperUtils;
import org.apache.curator.framework.CuratorFramework;

import java.util.Date;

/**
 * Author LTY
 * Date 2018/05/25
 */
public class Main {
    public static void main(String[] args) throws Exception {
        CuratorFramework zookeeperClient = ZookeeperUtils.getZookeeperClient();

        Date date = new Date();
        Cluster cluster = new Cluster();
        cluster.setName("redis_cluster_test");
        cluster.setAddTime(date);
        cluster.setLastModifyTime(date);
        cluster.setStatus(ClusterStatus.NORMAL);
        cluster.setShardStrategy(ClusterShardStrategy.MOD);
        zookeeperClient.create().forPath("/clusters/redis_cluster_test", JsonUtils.toJSON(cluster).getBytes());

        Node node = new Node();
        node.setClusterName("redis_cluster_test");
        node.setName("redis_cluster_1_node_1");
        node.setStatus(NodeStatus.ACTIVE);
        node.setWriteStrategy(NodeWriteStrategy.MASTER);
        node.setReadStrategy(NodeReadStrategy.MASTER);
        node.setStart(0L);
        node.setEnd(1000000000L);
        node.setSecondShardKey(1);
        node.setAddTime(date);
        node.setLastModifyTime(date);
        RegistryService.registerNode("redis_cluster_test", ClusterType.REDIS,node,false,false);


        Instance instance = new Instance();
        instance.setNodeName("redis_cluster_1_node_1");
        instance.setName("instance_m");
        instance.setStatus(ActiveStatus.ACTIVE);
        instance.setPort(6379);
        instance.setHostName("localhost");
        instance.setDomain("127.0.0.1");
        instance.setAddTime(date);
        instance.setLastModifyTime(date);
        instance.setMsStatus(MasterSlaveStatus.MASTER);
        RegistryService.registerInstance("redis_cluster_test",ClusterType.REDIS,"redis_cluster_1_node_1",instance,false,false);

        StoreDaoFactory.createStoreDao("redis_cluster_test").set("1","key-test","value-test");
    }
}
