package com.redis.store.hash;

import com.redis.store.cluster.Node;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HexPrefixModLocator implements IHashLocator {
    private static Logger logger = Logger.getLogger(HexPrefixModLocator.class);

    private static final Map<Character, Integer> MAP;

    static {
        MAP = new HashMap<>();
        MAP.put('0', 0);
        MAP.put('1', 1);
        MAP.put('2', 2);
        MAP.put('3', 3);
        MAP.put('4', 4);
        MAP.put('5', 5);
        MAP.put('6', 6);
        MAP.put('7', 7);
        MAP.put('8', 8);
        MAP.put('9', 9);
        MAP.put('A', 10);
        MAP.put('B', 11);
        MAP.put('C', 12);
        MAP.put('D', 13);
        MAP.put('E', 14);
        MAP.put('F', 15);
    }

    private List<Node> allNodes;

    HexPrefixModLocator(List<Node> nodes) {
        this.allNodes = nodes;
    }

    @Override
    public Node getNodeByKey(String hashKey) {
        Integer prefix = MAP.get(hashKey.charAt(0));
        if (prefix == null) {
            logger.error("Get node error for: " + hashKey);
            return null;
        }

        int index = prefix % allNodes.size();
        return allNodes.get(index);
    }

    @Override
    public void addNode(Node node) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeNode(Node node) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNodes(List<Node> nodes) {
        synchronized (this) {
            this.allNodes = nodes;
        }
    }

    @Override
    public String toString() {
        return "PrefixModLocator [allNodes=" + allNodes + "]";
    }

    @Override
    public void removeNode(Node node, boolean noPing) {
        throw new UnsupportedOperationException();
    }
}
