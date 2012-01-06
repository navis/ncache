package com.nexr.cache;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ConnectionManager {

    private String clusterName;

    private Map<String, ServerEntity> servers;     // servername --> server
    private ServerEntity[] sindex;                 // sindex --> sever

    public ConnectionManager(String clusterName) {
        this.clusterName = clusterName;
        this.servers = new HashMap<String, ServerEntity>();
        this.sindex = new ServerEntity[4];
    }

    public synchronized int serverNum() {
        return sindex.length;
    }

    public synchronized void syncServer(List<String> children) {
        Set<String> remain = new HashSet<String>(servers.keySet());
        for (String child : children) {
            IndexedServerName name = PartitionUtil.parseServerName(child);
            ServerEntity prev = servers.get(name.name);
            if (prev == null) {
                ServerEntity server = new ServerEntity(name);
                server.setConnection(newConnection(server));
                setServerIndex(server.index(), server);
                servers.put(name.name, server);
            }
            remain.remove(name.name);
        }
        for (String name : remain) {
            ServerEntity server = servers.remove(name);
            if (server != null) {
                setServerIndex(server.index(), null);
                server.shutdown();
            }
        }
    }

    private void setServerIndex(int index, ServerEntity manager) {
        if (sindex.length >= index) {
            ServerEntity[] newIndex = new ServerEntity[Math.max(sindex.length << 1, index)];
            System.arraycopy(sindex, 0, newIndex, 0, sindex.length);
            sindex = newIndex;
        }
        sindex[index] = manager;
    }

    public synchronized Set<String> addedNames(List<String> current) {
        Set<String> duplicate = new HashSet<String>(current);
        duplicate.removeAll(servers.keySet());
        return duplicate;
    }

    public synchronized Set<String> removedNames(List<String> current) {
        Set<String> remain = new HashSet<String>(servers.keySet());
        remain.removeAll(current);
        return remain;
    }

    public synchronized ServerEntity getServer(String name) {
        return servers.get(name);
    }

    public synchronized ServerEntity serverFor(int index) {
        ServerEntity server = sindex[index];
        if (server == null) {
            throw new IllegalStateException("invalid server index " + index);
        }
        return server;
    }

    public synchronized ServerEntity serverFor(String name) {
        ServerEntity server = servers.get(name);
        if (server == null) {
            throw new IllegalStateException("invalid server name " + name);
        }
        return server;
    }

    public synchronized void destroy() {
        for (ServerEntity server : servers.values()) {
            server.shutdown();
        }
    }

    public synchronized ServerEntity[] servers() {
        return Arrays.copyOf(sindex, servers.size());
    }

    public String serverConfigPath() {
        return Names.serverConfigPath(clusterName);
    }

    public String nspaceConfigPath() {
        return Names.nspaceConfigPath(clusterName);
    }

    public String serverRuntimePath() {
        return Names.serverRuntimePath(clusterName);
    }

    public String nspaceRuntimePath() {
        return Names.nspaceRuntimePath(clusterName);
    }

    public String serverRuntimePath(String server) {
        return Names.serverRuntimePath(clusterName, server);
    }

    public String nspaceRuntimePath(String nspace) {
        return Names.nspaceRuntimePath(clusterName, nspace);
    }

    public String managerPath() {
        return Names.managerPath(clusterName);
    }

    protected abstract ConnectionHandler newConnection(ServerEntity server);
}
