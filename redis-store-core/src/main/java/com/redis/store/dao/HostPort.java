package com.redis.store.dao;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Author LTY
 * Date 2018/05/23
 */
public class HostPort {

    private String host;
    private int port;

    public HostPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Ping host:port to check its alive.
     */
    public void ping() throws Exception {
        Socket s = null;
        try {
            s = new Socket();
            SocketAddress endpoint = new InetSocketAddress(host, port);
            s.connect(endpoint, 1000); // timeout 1000ms
        } finally {
            if (s != null)
                s.close();
        }
    }

    public boolean isAvailable() {
        try {
            ping();
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    public static boolean isValid(String hostport) {
        try {
            parse(hostport);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * @param hostPort
     *            192.168.1.1:6359
     */
    public static HostPort parse(String hostPort) {
        return new HostPort(hostPort.split(":")[0], Integer.valueOf(hostPort.split(":")[1]));
    }

    /**
     * @param hostPorts
     *            [192.168.1.1:6359, 192.168.1.2:6359]
     */
    public static List<HostPort> parse(Collection<String> hostPorts) {
        List<HostPort> list = new ArrayList<>();
        for (String hostPort : hostPorts) {
            list.add(parse(hostPort));
        }
        return list;
    }

    public static List<String> toString(Collection<HostPort> hostPorts) {
        List<String> list = new ArrayList<>();
        if (hostPorts != null) {
            for (HostPort hostport : hostPorts) {
                list.add(hostport.toString());
            }
        }
        return list;
    }

    /**
     * Do not modify it!
     */
    @Override
    public String toString() {
        return host + ":" + port;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + port;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        HostPort other = (HostPort) obj;
        if (host == null) {
            if (other.host != null)
                return false;
        } else if (!host.equals(other.host))
            return false;
        return port == other.port;
    }
}
