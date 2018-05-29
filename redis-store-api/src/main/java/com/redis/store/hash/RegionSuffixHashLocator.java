package com.redis.store.hash;

import com.redis.store.cluster.Node;
import org.apache.log4j.Logger;

import java.util.List;

public class RegionSuffixHashLocator extends RegionHashLocator {

    private static Logger logger = Logger.getLogger(RegionSuffixHashLocator.class);

    public RegionSuffixHashLocator(List<Node> nodes) {
        super(nodes);
    }

    /**
     * <tt>(key % size)</tt> must be less than <tt>size</tt>.
     */
    @Override
    public Node getNodeByKey(String hashKey) {
        long key = Math.abs(Long.valueOf(hashKey));
        long suffix = key % 10;

        if (availableNodes.isEmpty()) {
            return null;
        }

        for (Node node : allNodes) {
            if (node.getStart() <= key && node.getEnd() > key
                    && node.getSecondShardKey() == suffix)
                return node;
        }
        logger.error("fail to get hash node, the hash key:" + hashKey + " the suffix is:" + suffix);
        return null;
    }

}