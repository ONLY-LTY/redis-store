package com.redis.store.hash;

import com.redis.store.cluster.Cluster;
import com.redis.store.cluster.Node;
import com.redis.store.constants.ClusterShardStrategy;

import java.util.List;


public class ClusterHashLocator {
    private ClusterShardStrategy strategy;

    private IHashLocator hashLocator;

    public ClusterHashLocator(Cluster cluster, ClusterShardStrategy strategy) {
        this.strategy = strategy;

        //sort the node by node name
        List<Node> nodeList = cluster.getActiveNodes();
        nodeList.sort((node1, node2) -> {
            if (node1.getStart() != node2.getStart()) {
                return (int) (node1.getStart() - node2.getStart());
            }

            if (node1.getSecondShardKey() != node2.getSecondShardKey()) {
                return (int) (node1.getSecondShardKey() - node2.getSecondShardKey());
            }

            return node1.getName().compareTo(node2.getName());
        });

        switch (strategy) {
            case CONSISTENT_HASH:
                hashLocator = new KetamaHashLocator(nodeList);
                break;
            case MOD:
                hashLocator = new ModLocator(nodeList);
                break;
            case REGION:
                hashLocator = new RegionHashLocator(nodeList);
                break;
            case REGION_SUFFIX:
                hashLocator = new RegionSuffixHashLocator(nodeList);
                break;
            case HEX_PREFIX_MOD:
                hashLocator = new HexPrefixModLocator(nodeList);
                break;
            default:
                break;
        }

    }

    public Node getHashNode(String hashKey) {
        return hashLocator.getNodeByKey(hashKey);
    }

    public ClusterShardStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(ClusterShardStrategy strategy) {
        this.strategy = strategy;
    }

    public IHashLocator getHashLocator() {
        return hashLocator;
    }
}
