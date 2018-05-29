package com.redis.store.hash;

import com.redis.store.cluster.Node;
import com.redis.store.constants.NodeStatus;
import org.apache.log4j.Logger;

import java.util.*;


public class KetamaHashLocator implements IHashLocator {
    private static Logger logger = Logger.getLogger(KetamaHashLocator.class);
    protected static final int NUM_REPS = 160;

    protected final HashAlgorithm hashAlg;
    protected final Set<Node> nodes = Collections.synchronizedSet(new HashSet<Node>());
    protected transient volatile TreeMap<Long, Node> ketamaNodes = new TreeMap<>();

    public KetamaHashLocator(Collection<Node> nodes) {
        this.hashAlg = HashAlgorithm.KETAMA_HASH;
        this.nodes.addAll(nodes);

        buildMap();
    }

    @Override
    public Node getNodeByKey(String hashKey) {
        long hash = this.hashAlg.hash(hashKey);
        return getNodeByHash(hash);
    }

    @Override
    public synchronized void addNode(Node node) {
        if (!nodes.contains(node)) {
            nodes.add(node);
            buildMap();
        }
    }

    @Override
    public synchronized void removeNode(Node node) {
        if (nodes.size() <= 1) {
            return;
        }

        if (nodes.contains(node)) {
            nodes.remove(node);
            buildMap();
        }
    }

    @Override
    public synchronized void updateNodes(List<Node> nodes) {
        this.nodes.clear();
        this.nodes.addAll(nodes);
        buildMap();
    }

    protected synchronized void buildMap() {

        TreeMap<Long, Node> nodeMap = new TreeMap<>();
        for (Node node : nodes) {
            /** Duplicate 160 X weight references */
            if (hashAlg == HashAlgorithm.KETAMA_HASH) {
                this.compute(NUM_REPS, nodeMap, node);
            } else {
                for (int i = 0; i < NUM_REPS; i++) {
                    long key = hashAlg.hash(node.getName() + "-" + i);
                    nodeMap.put(key, node);
                }
            }
        }
        this.ketamaNodes = nodeMap;
    }

    @Override
    public String toString() {
        return "KetamaHashLocator [nodes=" + nodes + "]";
    }

    private Node getNodeByHash(long hash) {
        TreeMap<Long, Node> nodeMap = this.ketamaNodes;
        if (nodeMap == null || nodeMap.size() == 0) {
            return null;
        }

        Long resultHash = hash;
        if (nodeMap.containsKey(hash)) {
            Node resultNode = nodeMap.get(resultHash);
            if (resultNode.getStatus() == NodeStatus.ACTIVE)
                return resultNode;

        }

        int i = 0;
        SortedMap<Long, Node> tailMap = nodeMap.tailMap(hash);
        if (tailMap.isEmpty()) {
            tailMap = nodeMap.tailMap(0L);
        }

        Iterator<Long> it = tailMap.keySet().iterator();
        while (it.hasNext()) {
            long tmpHash = it.next();
            Node node = tailMap.get(tmpHash);
            if (node.getStatus() == NodeStatus.ACTIVE) {
                resultHash = tmpHash;
                break;
            }
            i++;
        }

        Node node = nodeMap.get(resultHash);
        if (node == null) {
            logger.error("fail to get hash node for the:" + hash);
            return null;
        }

        logger.debug("get hash node the node is:" + node.getName()
                + "the status is:" + node.getStatus() + " the i is:" + i);
        return node;
    }

    @Override
    public void removeNode(Node node, boolean noPing) {
        // TODO Auto-generated method stub

    }

    protected void compute(int weight, TreeMap<Long, Node> nodeMap, Node node) {
        for (int i = 0; i < weight / 4; i++) {
            byte[] digest = HashAlgorithm.computeMd5(node.getName()
                    + "-" + i);
            for (int h = 0; h < 4; h++) {
                long key = (long) (digest[3 + h * 4] & 0xFF) << 24
                        | (long) (digest[2 + h * 4] & 0xFF) << 16
                        | (long) (digest[1 + h * 4] & 0xFF) << 8
                        | digest[h * 4] & 0xFF;
                nodeMap.put(key, node);
            }
        }
    }


}
