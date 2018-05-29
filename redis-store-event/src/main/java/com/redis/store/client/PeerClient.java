package com.redis.store.client;

import com.google.common.annotations.VisibleForTesting;
import com.redis.store.constants.ClusterType;
import com.redis.store.constants.Constants;
import com.redis.store.constants.ReHashStatus;
import com.redis.store.events.ClusterRehashEvent;
import com.redis.store.events.StoreEvent;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.util.concurrent.LinkedBlockingQueue;

import static com.redis.store.util.ZkClientUtils.getZkClient;


/**
 * 控制zookeeper中本机状态
 */
public class PeerClient {

    private static final Logger LOG = Logger.getLogger(PeerClient.class);

    private static final Logger ZK_EVENTS_LOG = Logger.getLogger(PeerClient.class);

    private final String controlChannelPath;

    private final String statusChannelPath;

    private final LinkedBlockingQueue<StoreEvent> events;

    private int changedCount = 0;

    public PeerClient(ClusterType clusterType, String clusterName, String clientName, LinkedBlockingQueue<StoreEvent> events) {
        this(getClusterClientsStatusPath(clusterType, clusterName), clientName, events);
    }

    @VisibleForTesting
    PeerClient(String controlChannelPath, String clientName, LinkedBlockingQueue<StoreEvent> events) {
        this.controlChannelPath = controlChannelPath;
        statusChannelPath = controlChannelPath + "/" + clientName;

        this.events = events;

        createPath();
        subscribeControlChannel();
        subscribeStateChanges();
    }

    public boolean refreshStatus(ReHashStatus status) {
        try {
            getZkClient().writeData(statusChannelPath, status.toString(), changedCount++);
            return true;
        } catch (Exception e) {
            LOG.error("Ops", e);
        }
        return false;
    }

    private void createPath() {
        if (!getZkClient().exists(controlChannelPath)) {
            getZkClient().createPersistent(controlChannelPath, ReHashStatus.NORMAL.name());
        }
        if (getZkClient().exists(statusChannelPath)) {
            LOG.warn("client " + statusChannelPath + " already existed!");
            return;
        }
        getZkClient().createEphemeral(statusChannelPath, ReHashStatus.NORMAL.name());
    }

    private void subscribeControlChannel() {
        getZkClient().subscribeDataChanges(controlChannelPath, new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) {
                ZK_EVENTS_LOG.info("[Control][Change] " + dataPath + " " + data);
                for (ReHashStatus status : ReHashStatus.values()) {
                    if (status.toString().equalsIgnoreCase(data.toString())) {
                        events.offer(new ClusterRehashEvent(status));
                        return;
                    }
                }
            }

            @Override
            public void handleDataDeleted(String dataPath) {
                // 不该被触发的事件
                ZK_EVENTS_LOG.fatal("[Control][Delete] " + dataPath);
            }
        });
    }

    private void subscribeStateChanges() {
        getZkClient().subscribeStateChanges(new IZkStateListener() {

            @Override
            public void handleStateChanged(KeeperState state) {
            }

            @Override
            public void handleSessionEstablishmentError(Throwable error) {
            }

            @Override
            public void handleNewSession() {
                createPath();
            }
        });
    }

    public static String getClusterClientsStatusPath(ClusterType clusterType, String clusterName) {
        return Constants.CLIENTS_STATUS_ROOT_PATH + "/" + clusterType.name() + "/" + clusterName;
    }
}
