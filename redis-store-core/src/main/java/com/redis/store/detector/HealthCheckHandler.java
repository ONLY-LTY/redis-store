package com.redis.store.detector;

import com.google.code.yanf4j.util.ConcurrentHashSet;
import com.redis.store.cluster.Node;
import com.redis.store.constants.NodeStatus;
import com.redis.store.dao.IRedisDao;
import com.redis.store.service.NamedThreadFactory;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.*;


public class HealthCheckHandler implements InvocationHandler {

    private static final Logger LOGGER = Logger.getLogger(HealthCheckHandler.class);

    private final String clusterName;
    private final Node node;
    private final IRedisDao redisDao;

    private static volatile boolean hasShutdown = true;

    private static final Semaphore availableResources = new Semaphore(5);
    private static final ConcurrentHashSet<String> proceedingAddSet = new ConcurrentHashSet<>();
    private static final ConcurrentHashSet<String> removedAddSet = new ConcurrentHashSet<>();
    private static final ExecutorService checkExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("check-proxy-alive"));
    private static final DelayQueue<RetryElement> retryDelayQueue = new DelayQueue<>();

    static {
        Thread retryThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted() && hasShutdown) {
                try {
                    RetryElement element = retryDelayQueue.take();
                    String clusterName = element.getClusterName();
                    Node node = element.getNode();
                    IRedisDao redisDao = element.getRedisDao();
                    String identityKey = genIdentityKey(clusterName, node, redisDao);
                    if (node.getStatus() == NodeStatus.ACTIVE && redisDao.ping()) {
                        removedAddSet.remove(identityKey);
                        LOGGER.warn("Remove identityKey from removedAddSet " + identityKey + " timestamp " + System.currentTimeMillis());
                    } else {
                        retryDelayQueue.offer(RetryElement.createRetryElement(clusterName, node, redisDao));
                    }
                } catch (InterruptedException e) {
                    LOGGER.error(e);
                }
            }
        });
        retryThread.setName("reconnect-removedAdd-thread");
        retryThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            hasShutdown = false;
            checkExecutor.shutdown();
            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                LOGGER.error(e);
            }
            checkExecutor.shutdownNow();
        }));
    }

    public HealthCheckHandler(String clusterName, Node node, IRedisDao redisDao) {
        this.clusterName = clusterName;
        this.node = node;
        this.redisDao = redisDao;
    }

    @Override
    public Object invoke(Object o, Method method, Object[] objects) throws Throwable {
        try {
            return method.invoke(redisDao, objects);
        } catch (Exception e) {
            final String identityKey = genIdentityKey(clusterName, node, redisDao);
            boolean hasRemoved = removedAddSet.contains(identityKey);
            boolean proceeding = proceedingAddSet.contains(identityKey);
            if (!hasRemoved && !proceeding) {
                proceedingAddSet.add(identityKey);
                boolean hasAcquired = false;
                try {
                    hasAcquired = availableResources.tryAcquire(0L, TimeUnit.SECONDS);
                    if (hasAcquired) {
                        checkExecutor.execute(() -> {
                            for (int checkTimes = 0; checkTimes < 3; checkTimes++) {
                                try {
                                    Thread.sleep(200 * checkTimes);
                                } catch (InterruptedException e1) {
                                    LOGGER.error(e1);
                                }
                                if (redisDao.ping()) {
                                    LOGGER.error("Ping error identityKey " + identityKey);
                                    break;
                                }
                                if (checkTimes >= 2) {
                                    removedAddSet.add(identityKey);
                                    retryDelayQueue.offer(RetryElement.createRetryElement(clusterName, node, redisDao));
                                    LOGGER.error("Add identityKey from removedAddSet " + identityKey + " timestamp " + System.currentTimeMillis());
                                }
                            }
                        });
                    }
                } catch (Exception e1) {
                    LOGGER.error(e);
                } finally {
                    if (hasAcquired) {
                        availableResources.release();
                    }
                    proceedingAddSet.remove(identityKey);
                }
            }
            throw e;
        }
    }

    private static class RetryElement implements Delayed {

        private final String clusterName;
        private final Node node;
        private final long nextReTryTimestamp;
        private final IRedisDao redisDao;

        private RetryElement(String clusterName, Node node, long nextReTryTimestamp, IRedisDao redisDao) {
            this.clusterName = clusterName;
            this.node = node;
            this.nextReTryTimestamp = nextReTryTimestamp;
            this.redisDao = redisDao;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(this.nextReTryTimestamp - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            RetryElement other = (RetryElement) o;
            if (this.nextReTryTimestamp > other.nextReTryTimestamp) {
                return 1;
            }
            return -1;
        }

        public IRedisDao getRedisDao() {
            return redisDao;
        }

        public String getClusterName() {
            return clusterName;
        }

        public Node getNode() {
            return node;
        }

        private static RetryElement createRetryElement(String clusterName, Node node, IRedisDao redisDao) {
            return new RetryElement(clusterName, node, System.currentTimeMillis() + 8000L, redisDao);
        }
    }

    private static String genIdentityKey(String cluster, Node node, IRedisDao redisDao) {
        return cluster + ":" + node.getName() + ":" + redisDao.getHostPort();
    }


}
