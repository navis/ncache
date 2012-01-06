package com.nexr.cache;

public interface ServerResource extends ResourceManager {

    void initialized(ServerConnectionManager cluster, MessageHandler service);

    ServerConnectionManager cluster();

    MessageHandler service();

    int clientID();
}
