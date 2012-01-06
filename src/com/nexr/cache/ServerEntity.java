package com.nexr.cache;

import java.util.Map;

import com.nexr.cache.message.Message;
import com.nexr.cache.message.MessageListener;
import com.nexr.cache.message.request.Request;

public class ServerEntity {

    final IndexedServerName name;

    ConnectionHandler handler;
    Map<String, String> addresses;

    volatile boolean active;

    public ServerEntity(IndexedServerName name) {
        this.name = name;
    }

    public void setConnection(ConnectionHandler handler) {
        this.handler = handler;
    }

    public boolean isActive() {
        return active;
    }

    public boolean isConnected() {
        return handler != null && !handler.isClosed();
    }

    public ServerEntity activate() {
        active = true;
        handler.awakeAll();
        return this;
    }

    public ServerEntity passivate() {
        active = false;
        handler.awakeAll();
        return this;
    }

    public int index() {
        return name.index;
    }

    public String name() {
        return name.name;
    }

    public ConnectionHandler connection() {
        return handler;
    }

    public void notify(Message message) {
        handler.notify(message);
    }

    public Request request(Message message) {
        return handler.request(message);
    }

    public Request request(Message message, Request request) {
        return handler.request(message, request);
    }

    public Request request(Message message, MessageListener listener) {
        return handler.request(message, listener);
    }

    public void updateAddress(Map<String, String> address) {
        this.addresses = address;
    }

    public String addressFor(String protocol) {
        return addresses == null ? null : addresses.get(protocol);
    }

    public void shutdown() {
        active = false;
        handler.shutdown();
    }

    public String toString() {
        return name + (active ? "[o]" : "[x]") + "=" + addresses;
    }

    public void markConflict(byte nspace, int partition) {
        if (handler != null) {
            handler.desynchronized(nspace, partition);
        }
    }
}
