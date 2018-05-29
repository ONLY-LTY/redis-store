package com.redis.store.cluster;

import com.redis.store.constants.ActiveStatus;
import com.redis.store.constants.MasterSlaveStatus;
import com.redis.store.constants.NodeReadStrategy;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.RandomUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class NodeUtils {

    public static List<Instance> getMasters(Node node) {
        return getInstanceExpect(node, MasterSlaveStatus.MASTER);
    }

    public static List<Instance> getSlaves(Node node) {
        return getInstanceExpect(node, MasterSlaveStatus.SLAVE);
    }

    private static List<Instance> getInstanceExpect(Node node, MasterSlaveStatus expected) {
        assert expected != null;

        if (node == null) {
            throw new NullPointerException("node");
        }
        if (node.getInstances() == null) {
            return Collections.emptyList();
        }

        List<Instance> instances = new ArrayList<>();
        for (Instance instance : node.getInstances()) {
            if (instance.getStatus() == ActiveStatus.DELETED) {
                continue;
            }
            MasterSlaveStatus status = instance.getMsStatus();
            if (status == MasterSlaveStatus.ALL || status == expected) {
                instances.add(instance);
            }
        }
        return instances;
    }

    public static List<Instance> findActiveInstances(Node node) {
        if (node == null) {
            throw new NullPointerException("node");
        }
        if (node.getInstances() == null) {
            return Collections.emptyList();
        }

        List<Instance> activeInstances = new ArrayList<>();
        for (Instance instance : node.getInstances()) {
            if (instance.getStatus() == ActiveStatus.DELETED) {
                continue;
            }
            activeInstances.add(instance);
        }
        return activeInstances;
    }

    public static List<Instance> findWriteInstances(Node node) {
        List<Instance> masters = NodeUtils.getMasters(node);
        if (CollectionUtils.isEmpty(masters)) {
            return Collections.emptyList();
        }

        List<Instance> writeInstances = new ArrayList<>();
        switch (node.getWriteStrategy()) {
            case MASTER:
                writeInstances.add(masters.get(0));
                break;
            case MUILTI_MASTER:
                writeInstances.addAll(masters);
                break;
            case RANDOM:
                writeInstances.add(masters.get(RandomUtils.nextInt(0, masters.size())));
                break;
        }
        return writeInstances;
    }

    public static List<Instance> findReadInstance(Node node) {
        List<Instance> instances = null;
        NodeReadStrategy readStrategy = node.getReadStrategy();
        switch (readStrategy) {
            case SLAVES:
                List<Instance> slaves = NodeUtils.getSlaves(node);
                if (slaves.size() > 1) {
                    Collections.shuffle(slaves);
                }

                List<Instance> writeInstances = NodeUtils.findWriteInstances(node);
                if (CollectionUtils.isNotEmpty(writeInstances)) {
                    slaves.add(writeInstances.get(RandomUtils.nextInt(0, writeInstances.size())));
                }

                instances = slaves;
                break;
            case MASTER:
                instances = NodeUtils.findWriteInstances(node);
                break;
            case RANDOM:
                instances = NodeUtils.findActiveInstances(node);
                if (instances.size() > 1) {
                    Collections.shuffle(instances);
                }
                break;
        }
        return instances;
    }

    public static Instance findInstanceByName(Node node, String name) {
        for (Instance instance : node.getInstances()) {
            if (name.equals(instance.getName())) {
                return instance;
            }
        }
        return null;
    }

    public static List<Instance> findAliveInstances(Node node) {
        List<Instance> aliveInstances = new ArrayList<>();
        for (Instance instance : node.getInstances()) {
            if (instance.checkInstanceActive()) {
                aliveInstances.add(instance);
            }
        }
        return aliveInstances;
    }

    private NodeUtils() {
        // blank
    }
}
