package com.nexr.cache.client;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;

import com.nexr.cache.ResourceManager;
import com.nexr.cache.ServerEntity;
import com.nexr.cache.ConnectionHandler;
import com.nexr.cache.Constants;
import com.nexr.cache.message.Message;
import com.nexr.cache.message.MessageFactory;
import com.nexr.cache.util.NetUtil;

public class ClientConnectionHandler extends ConnectionHandler {

    ServerEntity target;

    public ClientConnectionHandler(ServerEntity server, ResourceManager resource) {
        super(resource);
        this.target = server;
    }

    protected Message createMessage(byte opcode) {
        return MessageFactory.createClient(opcode);
    }

    @Override
    protected boolean needConnection() {
        return target.isActive();
    }

    protected Message service(Message message) {
        throw new IllegalStateException("not supported, yet");
    }

    protected boolean newConnection() throws IOException {
        String address = target.addressFor(Constants.NCACHE_ADDRESS);
        System.out.println("-- [ClientConnectionHandler/newConnection] try connection to " + address);
        SocketChannel socket = NetUtil.newChannel(address);
        DataOutput output = new DataOutputStream(Channels.newOutputStream(socket));
        output.writeInt(Message.NEGO_CLIENT);
        if (connected(target.index(), socket) && establishing(target.index(), socket)) {
            return true;
        }
        NetUtil.close(socket);
        return false;
    }

    public String toString() {
        return " --> " + target.name();
    }
}