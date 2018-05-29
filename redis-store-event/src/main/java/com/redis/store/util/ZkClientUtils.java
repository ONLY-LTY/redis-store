package com.redis.store.util;

import com.redis.store.constants.Constants;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher;

import java.io.FileInputStream;
import java.util.Map;

/**
 * luofucong on 14-8-21.
 */
public class ZkClientUtils {

    private static final Logger ZK_EVENTS_LOG = Logger.getLogger(ZkClientUtils.class);

    private static final int connectionTimeout = 5000;

    private static volatile ZkClient ZK_CLIENT;

    public static ZkClient getZkClient() {
        return getZkClient(null);
    }

    public static ZkClient getZkClient(String zkServers) {
        if (ZK_CLIENT == null) {
            synchronized (ZkClientUtils.class) {
                if (ZK_CLIENT == null) {
                    final String servers = StringUtils.isBlank(zkServers) ? getZkServers() : zkServers;
                    ZK_EVENTS_LOG.info("start to connect the zk center:" + servers);
                    ZkClient zkClient;
                    try {
                        zkClient = new ZkClient(servers, connectionTimeout);
                    } catch (Exception e) {
                        throw new RuntimeException("unable to connect to zookeeper " + servers, e);
                    }

                    // String serializer
                    zkClient.setZkSerializer(ZkStringSerializer.INSTANCE);

                    zkClient.subscribeStateChanges(new IZkStateListener() {

                        private volatile int retryWaitSec = 1;

                        @Override
                        public void handleStateChanged(Watcher.Event.KeeperState state) {
                            ZK_EVENTS_LOG.info("[State][Change] " + state);
                        }

                        @Override
                        public void handleNewSession() {
                            ZK_EVENTS_LOG.info("[State][NewSession]");
                        }

                        @Override
                        public void handleSessionEstablishmentError(Throwable error) {
                            ZK_EVENTS_LOG.error(" fatal error [State][Error] ", error);

                            // exponential back-off sleep
                            // TimeUnit.SECONDS.sleep(retryWaitSec *= 2);
                            // retry

                            //ZK_CLIENT = new ZkClient(servers);
                        }
                    });

                    ZK_CLIENT = zkClient;
                }
            }
        }
        return ZK_CLIENT;
    }

    static String getZkServers() {

        StringBuilder zkServers = new StringBuilder();
        PropertiesManager propertiesManager = null;
        String configPath = System.getProperty(Constants.REDIS_CONFIG_PATH_KEY_NAME);
        String configFileName = Constants.REDIS_CONFIG_ONLINE_FILE_NAME;

        if (!StringUtils.isEmpty(configPath)) {
            String filePath = configPath + "/" + configFileName;
            try {
                FileInputStream configIns = new FileInputStream(filePath);
                propertiesManager = new PropertiesManager(configIns);
            } catch (Exception e) {
                ZK_EVENTS_LOG.error("fail to parse the config ..the path is:" + filePath, e);
            }
        }

        if (propertiesManager == null) {
            ZK_EVENTS_LOG.info("use the default config file..." + configFileName);
            propertiesManager = new PropertiesManager(configFileName);
        }

        Map<String, Object> hosts = propertiesManager.getPropsByPrefix( "zookeeper_server_host", false);
        Map<String, Object> ports = propertiesManager.getPropsByPrefix( "zookeeper_server_port", false);
        if (hosts.entrySet().size() != ports.entrySet().size()) {
            throw new IllegalArgumentException( "Bad Zookeeper servers addresses!");
        }
        for (int i = 1, l = hosts.entrySet().size(); i <= l; ++i) {
            String key = String.valueOf(i);
            zkServers.append(hosts.get(key)).append(":").append(ports.get(key));
            if (i < l) {
                zkServers.append(",");
            }
        }
        return zkServers.toString();
    }
}
