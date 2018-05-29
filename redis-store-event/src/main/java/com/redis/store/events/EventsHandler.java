package com.redis.store.events;

import com.redis.store.listener.ClusterChangedListener;
import com.redis.store.listener.InstanceChangeListener;
import com.redis.store.listener.NodeChangedListener;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class EventsHandler implements Runnable {

    private static final Logger LOG = Logger.getLogger(EventsHandler.class);

    private final LinkedBlockingQueue<StoreEvent> events;

    private final List<ClusterChangedListener> clusterChangedListeners;

    private final List<NodeChangedListener> nodeChangedListeners;

    private final List<InstanceChangeListener> instanceChangeListeners;

    private final String clusterId;

    public EventsHandler(String clusterId, LinkedBlockingQueue<StoreEvent> events,
                         List<ClusterChangedListener> clusterChangedListeners,
                         List<NodeChangedListener> nodeChangedListeners,
                         List<InstanceChangeListener> instanceChangeListeners) {
        this.clusterId = clusterId;
        this.events = events;
        this.clusterChangedListeners = clusterChangedListeners;
        this.nodeChangedListeners = nodeChangedListeners;
        this.instanceChangeListeners = instanceChangeListeners;
    }

    @Override
    public void run() {
        Thread thread = Thread.currentThread();
        String threadNameStash = thread.getName();
        thread.setName("[EventsHandler-" + clusterId + "]");
        try {
            StoreEvent event = events.poll(10, TimeUnit.MILLISECONDS);
            if (event == null) {
                return;
            }
            LOG.info(event);

            switch (event.type) {
                case CLUSTER:
                    event.notify(clusterChangedListeners);
                    break;
                case NODE:
                    event.notify(nodeChangedListeners);
                    break;
                case INSTANCE:
                    event.notify(instanceChangeListeners);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown event " + event);
            }
        } catch (InterruptedException ie) {
            LOG.warn("[EventsHandler] Interrupt!");
        } catch (Exception e) {
            LOG.error("Ops", e);
        } finally {
            thread.setName(threadNameStash);
        }
    }
}
