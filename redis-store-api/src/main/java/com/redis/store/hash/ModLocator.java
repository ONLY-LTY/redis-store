package com.redis.store.hash;

import com.redis.store.cluster.Node;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;


public class ModLocator implements IHashLocator {
    private static Logger logger = Logger.getLogger(ModLocator.class);

    private List<Node> allNodes; // 所有节点的列表，一旦设置不可以进行修改。用于记录每个节点的位置。
    private List<Node> availableNodes; // 当前可用的节点列表。
    private final ReentrantLock lock = new ReentrantLock();

    public ModLocator(List<Node> nodes) {
        this.allNodes = Collections.unmodifiableList(nodes);
        this.availableNodes = new CopyOnWriteArrayList<>(nodes);
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

        int index = (int) (key % availableNodes.size());
        Node node = availableNodes.get(index);
        if (node == null) {
            logger.error("failt to get hash node, the hash key:" + hashKey);
        }
        return node;
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
        return "ModLocator [availableNodes=" + availableNodes + "]";
    }

    @Override
    public void removeNode(Node node, boolean noPing) {

    }

}
