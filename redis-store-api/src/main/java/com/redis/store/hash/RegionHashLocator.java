package com.redis.store.hash;

import com.redis.store.cluster.Node;
import com.redis.store.constants.NodeStatus;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class RegionHashLocator implements IHashLocator {
    private static Logger logger = Logger.getLogger(RegionHashLocator.class);

    protected List<Node> allNodes; // 所有节点的列表，一旦设置不可以进行修改。用于记录每个节点的位置。
    protected List<Node> availableNodes; // 当前可用的节点列表。
    protected final ReentrantLock lock = new ReentrantLock();

    public RegionHashLocator(List<Node> nodes) {
        this.allNodes = Collections.unmodifiableList(nodes);
        this.availableNodes = new ArrayList<>(nodes);
        this.availableNodes.sort((node1, node2) -> (int) (node1.getStart() - node2.getStart()));
    }

    /**
     * <tt>(key % size)</tt> must be less than <tt>size</tt>.
     */
    @Override
    public Node getNodeByKey(String hashKey) {
        long key = Math.abs(Long.valueOf(hashKey));

        if (availableNodes.isEmpty()) {
            return null;
        }
        for (Node node : allNodes) {
            if (node.getStatus() != NodeStatus.ACTIVE)
                continue;
            if (node.getStart() <= key && node.getEnd() > key)
                return node;
        }
        logger.error("failt to get hash node, the hash key:" + hashKey);
        return null;
    }

    /**
     * 添加节点。<br />
     * 尽量保证原始的List顺序。
     */
    @Override
    public void addNode(Node node) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (allNodes.contains(node) && !availableNodes.contains(node)) {
                int index = allNodes.indexOf(node);
                if (index < availableNodes.size()) {
                    availableNodes.add(index, node);
                } else { // Avoid IndexOutOfBoundsException.
                    availableNodes.add(node);
                }

                return;
            }
        } finally {
            lock.unlock();
        }

    }

    @Override
    public void removeNode(Node node) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (availableNodes.size() > 1) {
                return;
            }

            if (availableNodes.contains(node)) {
                availableNodes.remove(node);
                return;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void updateNodes(List<Node> nodes) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            allNodes.clear();
            allNodes = Collections.unmodifiableList(nodes);

            availableNodes.clear();
            availableNodes.addAll(nodes);

        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return "RegionHashLocator [availableNodes=" + availableNodes + "]";
    }

    @Override
    public void removeNode(Node node, boolean noPing) {

    }
}