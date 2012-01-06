package com.nexr.cache;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.EOFException;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import com.nexr.cache.ServerResource;
import com.nexr.cache.Constants;
import com.nexr.cache.message.Message;
import com.nexr.cache.util.NetUtil;

public class ServiceListener implements Runnable {

    volatile boolean service;
    volatile boolean shutdown;

    String address;
    ServerResource resource;
    ServerConnectionManager cluster;
    ServerSocketChannel ssocket;

    public ServiceListener(ServerResource resource, String address) throws IOException {
        this.resource = resource;
        this.address = address;
        this.cluster = resource.cluster();
        this.ssocket = NetUtil.serverChannel(address);
    }

    public ServiceListener start() {
        resource.execute(this);
        return this;
    }

    public ServiceListener shutdown() {
        shutdown = true;
        NetUtil.close(ssocket);
        return this;
    }

    public void run() {
        while (!shutdown) {
            try {
                SocketChannel socket = ssocket.accept();
                socket.socket().setSoLinger(false, 0);
                socket.socket().setTcpNoDelay(true);
                negotiate(socket);
            } catch (ClosedChannelException e) {
                try {
                    ssocket = NetUtil.serverChannel(address);
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void negotiate(SocketChannel socket) {
        try {
            DataInput input = new DataInputStream(Channels.newInputStream(socket));
            DataOutput output = new DataOutputStream(Channels.newOutputStream(socket));

            int negotitaion = input.readInt();
            switch(negotitaion) {
                case Message.NEGO_CLIENT:
                    if (!serviceAwait(Constants.INITIAL_SYNC_TIMEOUT)) {
                        throw new IllegalStateException("service not ready");
                    }
                    ServiceConnectionHandler client = new ServiceConnectionHandler(resource);
                    if (!client.accepted(-client.clientID(), socket) || !client.establishing(-client.clientID(), socket)) {
                        NetUtil.close(socket);
                    }
                    break;
                case Message.NEGO_PEER:
                    int index = input.readInt();
                    ConnectionHandler server = cluster.connectionFor(index);
                    boolean success = server.accepted(index, socket, index > cluster.index());
                    output.writeInt(success ? Message.NEGO_SUCCESS : Message.NEGO_FAILURE);
                    if (!success || input.readInt() != Message.NEGO_SUCCESS || !server.establishing(index, socket)) {
                        NetUtil.close(socket);
                    }
                    break;
                default:
                    throw new IllegalStateException("invalid negotiation " + negotitaion);
            }
        } catch (Exception e) {
            System.out.println("[ServiceListener/negotiate] failed by exception :: " + e);
            if (!(e instanceof EOFException) && !(e instanceof ClosedChannelException)) {
                e.printStackTrace();
            }
            NetUtil.close(socket);
        }
    }

    public synchronized boolean serviceAwait(long timeout) throws InterruptedException {
        while (!service) {
            wait(timeout);
        }
        return service;
    }

    public synchronized void acceptClients() {
        System.out.println("-- [ServiceListener/acceptClients] !!!");
        service = true;
        notifyAll();
    }
}
