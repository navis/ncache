package com.nexr.cache.client;

import com.nexr.cache.ConnectionManager;
import com.nexr.cache.ResourceManager;
import com.nexr.cache.ServerEntity;
import com.nexr.cache.ConnectionHandler;

public class ClientConnectionManager extends ConnectionManager {

    ResourceManager resource;

    public ClientConnectionManager(String clusterName, ResourceManager resource) {
        super(clusterName);
        this.resource = resource;
    }

    protected ConnectionHandler newConnection(ServerEntity server) {
        ConnectionHandler connection = new ClientConnectionHandler(server, resource);
        connection.startTransceiver();
        return connection;
    }
}
