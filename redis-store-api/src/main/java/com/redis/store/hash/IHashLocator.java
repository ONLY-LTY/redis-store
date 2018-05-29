package com.redis.store.hash;


import com.redis.store.cluster.Node;

import java.util.List;


public interface IHashLocator {

    /**
     * 根据HashKey获取节点。
     */
    Node getNodeByKey(String hashKey);

    /**
     * 添加节点。
     */
    void addNode(Node node);

    /**
     * 移除节点。
     */
    void removeNode(Node node);
    
    /**
     * 移除节点，不需要ping
     * @param node
     * @param noPing
     */
    void removeNode(Node node, boolean noPing);
    
    /**
     * 更新节点列表。
     */
    void updateNodes(List<Node> nodes);

}