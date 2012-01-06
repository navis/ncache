package com.nexr.cache;

import java.nio.channels.SocketChannel;

import com.nexr.cache.MessageHandler;
import com.nexr.cache.ServerResource;
import com.nexr.cache.message.Message;
import com.nexr.cache.message.MessageFactory;

public class ServiceConnectionHandler extends ConnectionHandler {

    final int clientID;
    final MessageHandler handler;

    public ServiceConnectionHandler(ServerResource resource) {
        super(resource);
        handler = resource.service();
        clientID = resource.clientID();
    }

    public int clientID() {
        return clientID;
    }

    @Override
    protected synchronized boolean newConnection(SocketChannel socket, boolean priority) {
        if (super.newConnection(socket, priority)) {
            startTransceiver();
            return true;
        }
        return false;
    }

    protected Message service(Message message) {
        return handler.handle(message);
    }

    protected Message createMessage(byte opcode) {
        return MessageFactory.createServer(opcode);
    }

    protected boolean newConnection() {
        shutdown();
        return true;
    }

    public String toString() {
        return "CLIENT-" + clientID;
    }
}
