package com.nexr.cache;

import java.util.List;

public class ServerConnectionManager extends ConnectionManager {

    FullName name;
    ServerResource resource;

    ServerEntity self;       // self
    ServerEntity manager;

    public ServerConnectionManager(FullName name, ServerResource resource) {
        super(name.cluster());
        this.name = name;
        this.resource = resource;
    }

    public FullName name() {
        return name;
    }

    public int index() {
        return self.index();
    }

    public boolean isReady() {
        return self != null && manager != null;
    }

    public boolean isSelf(ServerEntity server) {
        return self == server;
    }

    public ServerEntity setManager(String name) {
        System.out.println("-- [ClusterManager/setManager] " + name);
        return manager = serverFor(name);
    }

    public ServerEntity self() {
        return self;
    }

    public ServerEntity manager() {
        return manager;
    }

    public boolean isManager() {
        return self == manager;
    }

    public synchronized ServerEntity initEntity(List<String> children) {
        syncServer(children);
        return self = serverFor(name.server());
    }

    public ConnectionHandler connectionFor(int index) {
        return serverFor(index).connection();
    }

    public ConnectionHandler connectionFor(String name) {
        return serverFor(name).connection();
    }

    protected ConnectionHandler newConnection(ServerEntity server) {
        PeerConnectionHandler connection = new PeerConnectionHandler(server, resource);
        if (!name.isSameServer(server.name())) {
            connection.startTransceiver();
        }
        return connection;
    }

    public String toString() {
        return name + (manager == null ? "" : "[" + manager.name() + "]");
    }
}
