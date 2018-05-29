package com.redis.store.proxy;

import com.redis.store.cluster.Cluster;
import com.redis.store.cluster.Instance;
import com.redis.store.cluster.Node;
import com.redis.store.cluster.NodeUtils;
import com.redis.store.cluster.factory.AbstractClusterWrapper;
import com.redis.store.constants.ClusterType;
import com.redis.store.constants.MasterSlaveStatus;
import com.redis.store.constants.NodeStatus;
import com.redis.store.constants.ReHashStatus;
import com.redis.store.dao.HostPort;
import com.redis.store.dao.IRedisDao;
import com.redis.store.dao.SimpleRedisDao;
import com.redis.store.hash.ClusterHashLocator;
import com.redis.store.service.RedisStoreService;
import com.redis.store.utils.ClientUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.redis.store.proxy.DefaultJedisPoolConfig.CONFIG_MEDIUM;

/**
 * Author LTY
 * Date 2018/05/24
 */
abstract class AbstractStoreProxy {

    private static final Logger LOGGER = Logger.getLogger(AbstractStoreProxy.class);
    private static final int DEFAULT_CONN_INIT_NUM = 10;
    private static final int CONFIG_LOAD_RETRY_NUM = 3;

    static final Map<String, IRedisDao> REDIS_DAO_MAP = new ConcurrentHashMap<>();
    ClusterHashLocator hashLocator;
    Cluster cluster;
    private AbstractClusterWrapper configWrapperProxy = null;
    private ReHashStatus status;
    private ClusterType clusterType;
    private String password;
    private String clusterName;
    private JedisPoolConfig poolConfig;
    private int poolInitConnNum;
    private int timeout;


    AbstractStoreProxy(String clusterName, String password, JedisPoolConfig poolConfig, int poolInitConnNum, int timeout, ClusterType clusterType) {
        if (StringUtils.isEmpty(clusterName)) {
            throw new IllegalArgumentException("cluster name is empty.");
        }
        if (poolConfig == null) {
            poolConfig = CONFIG_MEDIUM;
        }
        if (poolInitConnNum <= 0) {
            poolInitConnNum = DEFAULT_CONN_INIT_NUM;
        }

        if (timeout <= 0) {
            timeout = Protocol.DEFAULT_TIMEOUT;
        }

        this.clusterType = clusterType;
        this.clusterName = clusterName;
        this.poolConfig = poolConfig;
        this.poolInitConnNum = poolInitConnNum;
        this.password = password;
        this.timeout = timeout;

        try {
            init();
        } catch (Exception e) {
            if (configWrapperProxy != null) {
                try {
                    configWrapperProxy.close();
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            }
            throw new RuntimeException(e);
        }
    }

    private void init() {
        try {
            this.configWrapperProxy = new RedisStoreService(ClientUtils.DEFAULT_CLIENT_NAME, clusterName, clusterType);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ZkDataChange zkDataChange = new ZkDataChange(this);
        configWrapperProxy.registerClusterChangedListener(zkDataChange);
        configWrapperProxy.registerInstanceChangedListener(zkDataChange);
        configWrapperProxy.registerNodeChangedListener(zkDataChange);

        this.cluster = loadClusterConfig();
        if (this.cluster == null) {
            throw new RuntimeException("fail to load redis cluster config, the cluster name is:" + clusterName);
        }
        initCluster(this.cluster);
        clusterRehash();
    }

    public synchronized void initJedisPool(Instance instance, boolean forcedRebuild) {
        String hostPort = instance.findHostPort();
        if (!REDIS_DAO_MAP.containsKey(hostPort) || forcedRebuild) {
            LOGGER.info("start to init pool  for the cluster...the cluster name is:" + cluster.getName() + " the hostPort is:" + hostPort + " forcedRebuild :" + forcedRebuild);
            String[] hostPorts = hostPort.split(":");
            boolean isInitConnSucceed = true;
            // 预先创建一些链接
            try {
                List<Jedis> jedisList = new ArrayList<>();
                HostPort hostport = new HostPort(hostPorts[0], Integer.parseInt(hostPorts[1]));
                JedisPool pool = buildJedisPool(hostport.getHost(), hostport.getPort());

                int failCount = 0;
                for (int i = 0; i < this.poolInitConnNum; i++) {
                    try {
                        jedisList.add(pool.getResource());
                    } catch (Exception e) {
                        failCount++;
                        if (failCount >= 3) {
                            isInitConnSucceed = false;
                            LOGGER.error("fail to init conn:" + hostPort, e);
                            break;
                        }
                    }
                }
                for (Jedis jedis : jedisList) {
                    jedis.close();
                }

                IRedisDao dao = new SimpleRedisDao(pool).setHostPort(hostport);
                REDIS_DAO_MAP.put(hostPort, dao);
            } catch (Exception ex) {
                LOGGER.error("fail to create redis dao:" + hostPort, ex);
            }

            if (!isInitConnSucceed) {
                LOGGER.error("fail to init pool  for the cluster..."
                        + "the cluster name is:" + cluster.getName()
                        + " node name is:" + instance.getNodeName()
                        + " instance name is:" + instance.getName()
                        + " the hostPort is:" + hostPort);
                if (instance.getMsStatus() != MasterSlaveStatus.SLAVE) {
                    throw new IllegalStateException("fail to init pool " + "the cluster name is:" + cluster.getName() + " the hostPort is:" + hostPort);
                }
            }
        } else {
            LOGGER.info("the jedis pool is exists...the cluster name is:" + cluster.getName() + " the hostPort is:" + hostPort);
        }
    }

    private JedisPool buildJedisPool(String host, int port) {
        if (StringUtils.isNotBlank(password)) {
            return new JedisPool(poolConfig, host, port, timeout, password);
        } else {
            return new JedisPool(poolConfig, host, port, timeout);
        }
    }


    public synchronized void initCluster(Cluster cluster) {
        LOGGER.info("start to init the cluster redis pool . the cluster name is" + cluster.getName());
        for (Node node : cluster.getNodeList()) {
            if (node.getStatus() != NodeStatus.ACTIVE) {
                continue;
            }
            for (Instance instance : NodeUtils.findAliveInstances(node)) {
                initJedisPool(instance, false);
            }
        }
        LOGGER.info("finish to init the cluster redis pool . the cluster name is" + cluster.getName());
    }


    public synchronized void clusterRehash() {
        LOGGER.info("start to rehash for the cluster...the cluster name is:" + cluster.getName());
        this.hashLocator = new ClusterHashLocator(this.cluster, this.cluster.getShardStrategy());
    }

    public synchronized Cluster loadClusterConfig() {
        Cluster newCluster = null;
        for (int i = 0; i < CONFIG_LOAD_RETRY_NUM; i++) {
            try {
                newCluster = configWrapperProxy.loadCluster();
                break;
            } catch (Exception e) {
                if (i == (CONFIG_LOAD_RETRY_NUM - 1)) {
                    LOGGER.error("fail to load the cluster, the cluster name is" + clusterName, e);
                    configWrapperProxy.sendClusterStatus(ReHashStatus.FAIL);
                    return null;
                }
            }
        }
        return newCluster;
    }

    public synchronized void reloadCluster() {
        Cluster newCluster = configWrapperProxy.loadCluster();
        initCluster(newCluster);
        this.cluster = newCluster;
        clusterRehash();
    }

    public void removeJedisPoolByHostPort(String hostPort) {
        IRedisDao redisDao = REDIS_DAO_MAP.remove(hostPort);
        this.removeJedisPoolByRedisDao(redisDao);
    }

    public void removeJedisPoolByRedisDao(IRedisDao redisDao) {
        if (redisDao != null) {
            redisDao.destroy();
            LOGGER.info("finish to remove the redis conn the hostPort is:" + redisDao.toString());
        }
    }

    public void resumePubsubChannels(Instance newInstance, IRedisDao redisDao) {
        Map<JedisPubSub, String[]> pubSubChanelMap = redisDao.getJedisPubsubMap();
        if (pubSubChanelMap == null || pubSubChanelMap.size() == 0)
            return;
        Iterator<Map.Entry<JedisPubSub, String[]>> iterable = pubSubChanelMap.entrySet().iterator();
        IRedisDao redisDaoNew = REDIS_DAO_MAP.get(newInstance.findHostPort());
        Map.Entry<JedisPubSub, String[]> entry = null;
        while (iterable.hasNext()) {
            try {
                entry = iterable.next();
                if (entry == null || entry.getKey() == null) {//for safe
                    iterable.remove();
                    continue;
                }
                entry.getKey().unsubscribe();
            } catch (JedisConnectionException e) {
                LOGGER.error("unsubscribe failed, hostPort is: " + redisDao.getHostPort(), e);
                pubSubChanelMap.remove(entry.getKey());
            }
            redisDaoNew.subscribe(entry.getKey(), entry.getValue());
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public ReHashStatus getStatus() {
        return status;
    }

    public void setStatus(ReHashStatus status) {
        this.status = status;
    }

    public void sendClusterStatus(ReHashStatus status) {
        this.configWrapperProxy.sendClusterStatus(status);
    }

    public static IRedisDao getByHostPort(String hostPort) {
        return REDIS_DAO_MAP.get(hostPort);
    }

}
