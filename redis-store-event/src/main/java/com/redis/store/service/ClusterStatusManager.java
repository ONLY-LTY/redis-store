package com.redis.store.service;

import com.redis.store.client.PeerClient;
import com.redis.store.cluster.factory.IClusterStatusService;
import com.redis.store.constants.ClusterType;
import com.redis.store.constants.Constants;
import com.redis.store.constants.ReHashStatus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.redis.store.util.ZkClientUtils.getZkClient;


/**
 * luofucong on 14-8-25.
 */
public class ClusterStatusManager implements IClusterStatusService {

    private static final Logger LOG = Logger.getLogger(ClusterStatusManager.class);

    private static class Holder {
        private static final ClusterStatusManager INSTANCE = new ClusterStatusManager();
    }

    public static ClusterStatusManager getInstance() {
        return Holder.INSTANCE;
    }

    private ClusterStatusManager() {

    }

    @Override
    public ReHashStatus getClusterStatus(String clusterName) {
        if (StringUtils.isNotBlank(clusterName)) {
            try {
                String path = Constants.CLIENTS_STATUS_ROOT_PATH + File.separator + clusterName;
                
                String status = getZkClient().readData(path);
                return StringUtils.isBlank(status) ? null : ReHashStatus.valueof(status);
            } catch (Exception e) {
                LOG.error("Ops", e);
            }
        }
        return null;
    }

    @Override
    public synchronized void updateClusterStatus(String clusterName, ReHashStatus status) {
        if (StringUtils.isBlank(clusterName) || status == null) return;

        String path = Constants.CLIENTS_STATUS_ROOT_PATH + File.separator + clusterName;
        try {
            getZkClient().writeData(path, status);
        } catch (Exception e) {
            LOG.error("Ops", e);
        }
    }

    @Override
    public Map<String, ReHashStatus> getClientStatuses(String clusterName) {
        Map<String, ReHashStatus> clientStatuses = null;
        if (StringUtils.isNotBlank(clusterName)) {
            String path = Constants.CLIENTS_STATUS_ROOT_PATH + File.separator + clusterName;
            try {
                List<String> clients = getZkClient().getChildren(path);
                if (CollectionUtils.isNotEmpty(clients)) {
                    clientStatuses = new HashMap<>();
                    for (String client : clients) {
                        if (StringUtils.isNotBlank(client)) {
                            String clientPath = path + File.separator + client;
                            String status = getZkClient().readData(clientPath);
                            if (StringUtils.isNotBlank(status)) {
                                clientStatuses.put(client, ReHashStatus.valueof(status));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Ops", e);
            }
        }
        return clientStatuses;
    }

    @Override
    public synchronized void updateClientStatus(String clusterName, String clientName, ReHashStatus status) {
        if (StringUtils.isAnyBlank(clusterName, clientName) || status == null) return;

        String path = Constants.CLIENTS_STATUS_ROOT_PATH + File.separator + clusterName + File.separator + clientName;
        try {
            getZkClient().writeData(path, status);
        } catch (Exception e) {
            LOG.error("Ops", e);
        }
    }

    @Override
    public ReHashStatus getClusterStatus(ClusterType clusterType, String clusterName) {
        if (clusterType == null || StringUtils.isBlank(clusterName)) {
            LOG.warn("IllegalArguments: " + clusterType + ", " + clusterName);
            return null;
        }

        String path = PeerClient.getClusterClientsStatusPath(clusterType, clusterName);
        String status = null;
        try {
            status = getZkClient().readData(path);
        } catch (Exception e) {
            LOG.error("Ops", e);
        }
        return StringUtils.isBlank(status) ? null : ReHashStatus.valueof(status);
    }

    @Override
    public synchronized void updateClusterStatus(ClusterType clusterType, String clusterName, ReHashStatus status) {
        if (clusterType == null || StringUtils.isBlank(clusterName) || status == null) {
            LOG.warn("IllegalArguments: " + clusterType + ", " + clusterName + ", " + status);
            return;
        }

        String path = PeerClient.getClusterClientsStatusPath(clusterType, clusterName);
        try {
            getZkClient().writeData(path, status);
        } catch (Exception e) {
            LOG.error("Ops", e);
        }
    }

    @Override
    public Map<String, ReHashStatus> getClientStatuses(ClusterType clusterType, String clusterName) {
        if (clusterType == null || StringUtils.isBlank(clusterName)) {
            LOG.warn("IllegalArguments: " + clusterType + ", " + clusterName);
            return Collections.emptyMap();
        }

        Map<String, ReHashStatus> clientStatuses = null;
        String path = PeerClient.getClusterClientsStatusPath(clusterType, clusterName);
        try {
            List<String> clients = getZkClient().getChildren(path);
            if (CollectionUtils.isEmpty(clients)) {
                return Collections.emptyMap();
            }

            clientStatuses = new HashMap<>();
            for (String client : clients) {
                if (StringUtils.isBlank(client)) {
                    LOG.warn("found a blank client under " + path);
                } else {
                    String clientPath = path + File.separator + client;
                    String status = getZkClient().readData(clientPath);
                    if (StringUtils.isNotBlank(status)) {
                        clientStatuses.put(client, ReHashStatus.valueof(status));
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Ops", e);
        }
        return clientStatuses;
    }

    @Override
    public synchronized void updateClientStatus(ClusterType clusterType, String clusterName,
                                                String clientName, ReHashStatus status) {
        if (clusterType == null || StringUtils.isAnyBlank(clusterName, clientName) || status == null) {
            LOG.warn("IllegalArguments: " + clusterType + ", " + clusterName + ", " + clientName + ", " + status);
            return;
        }

        String path = PeerClient.getClusterClientsStatusPath(clusterType, clusterName) + File.separator + clientName;
        try {
            getZkClient().writeData(path, status);
        } catch (Exception e) {
            LOG.error("Ops", e);
        }
    }
}
