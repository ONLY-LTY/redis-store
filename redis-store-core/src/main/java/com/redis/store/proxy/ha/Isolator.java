package com.redis.store.proxy.ha;

/**
 * @author yangzhongjie
 *         Created:2016-11-29
 */
final class Isolator {

    private final String hostPort;

    private final long time;

    private volatile State state = State.ISOLATE;

    public Isolator(String hostPort, long time) {
        this.hostPort = hostPort;
        this.time = time;
    }

    public String getHostPort() {
        return hostPort;
    }

    public long getTime() {
        return time;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return "Isolator{" +
                "hostPort='" + hostPort + '\'' +
                ", time=" + time +
                '}';
    }

    static enum State {
        RECOVERED {
            @Override
            boolean isolated() {
                return false;
            }
        },

        ISOLATE {
            @Override
            boolean isolated() {
                return true;
            }
        };

        abstract boolean isolated();
    }
}
