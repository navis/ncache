package com.nexr.cache;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.HashSet;

import com.nexr.cache.message.Message;
import com.nexr.cache.message.MessageFactory;
import com.nexr.cache.message.SourceAware;
import com.nexr.cache.message.Desync;
import com.nexr.cache.util.NetUtil;

public class PeerConnectionHandler extends ConnectionHandler {

    ServerEntity server;
    ServerConnectionManager cluster;
    MessageHandler handler;

    Set<Desync.Entry> desync;

    public PeerConnectionHandler(ServerEntity server, ServerResource resource) {
        super(resource);
        this.server = server;
        this.cluster = resource.cluster();
        this.handler = resource.service();
        this.desync = new HashSet<Desync.Entry>();
    }

    protected Message service(Message message) {
        return handler.handle(message); 
    }

    protected Message createMessage(byte opcode) {
        Message message = MessageFactory.createServer(opcode);
        if (message instanceof SourceAware) {
            ((SourceAware)message).source(server);
        }
        return message;
    }

    protected boolean newConnection() throws IOException {
        if (server.isActive() && cluster.isReady()) {
            System.out.println("-- [PeerConnectionHandler/newConnection] " + server);
            SocketChannel socket = NetUtil.newChannel(server.addressFor(Constants.NCACHE_ADDRESS));
            if (!connected(server.index(), socket, cluster.index() > server.index())) {
                NetUtil.close(socket);
                return false;
            }
            DataOutput output = new DataOutputStream(Channels.newOutputStream(socket));
            DataInput input = new DataInputStream(Channels.newInputStream(socket));
            output.writeInt(Message.NEGO_PEER);
            output.writeInt(cluster.index());
            if (input.readInt() == Message.NEGO_SUCCESS && establishing(cluster.index(), socket)) {
                output.writeInt(Message.NEGO_SUCCESS);
                return true;
            }
            NetUtil.close(socket);
        }
        return false;
    }

    @Override
    public void desynchronized(byte nspace, int partitionID) {
        desync.add(new Desync.Entry(nspace, partitionID));
    }

    @Override
    public boolean establishing(int index, SocketChannel connected) {
        boolean established = super.establishing(index, connected);
        if (established && !desync.isEmpty()) {
            notify(new Desync(desync.toArray(new Desync.Entry[desync.size()])));
        }
        return established;
    }

    public String toString() {
        return cluster.index() + " --> " + server.index();
    }
}
