package com.nexr.cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import com.nexr.cache.message.*;
import com.nexr.cache.message.request.ListeningRequest;
import com.nexr.cache.message.request.Request;
import com.nexr.cache.message.request.RequestSingle;
import com.nexr.cache.util.NetUtil;

public abstract class ConnectionHandler {

    private static final int POLLING = 3000;
    private static final Message EOF = new EOF();

    private MemoryPool pool;
    private ResourceManager resource;

    private SocketChannel active;
    private boolean established;

    private Reader reader;
    private Writer writer;

    private volatile boolean shutdowning;
    private volatile boolean shutdowned;

    public ConnectionHandler(ResourceManager resource) {
        this.resource = resource;
        this.pool = resource.memory();
        this.reader = new Reader();
        this.writer = new Writer();
    }

    public ConnectionHandler startTransceiver() {
        resource.execute(reader);
        resource.execute(writer);
        return this;
    }

    public void notify(Message message) {
        writer.enqueueMessage(message);
    }

    public Request request(Message message) {
        return writer.request(message);
    }

    public Request request(Message message, Request request) {
        return writer.request(message, request);
    }

    public Request request(Message message, MessageListener listener) {
        return writer.request(message, listener);
    }

    public void shutdown() {
        if (!shutdowned && !shutdowning) {
            writer.enqueueMessage(EOF);
            shutdowning = true;
        }
        while (!isClosed()) {
            await(POLLING);
        }
        shutdowned = true;
    }

    private SocketChannel available(Transceiver executor) {
        while (!shutdowning && !established) {
            if (executor.establish(POLLING)) {
                break;
            }
        }
        return shutdowned ? null : active;
    }

    private boolean connect() {
        try {
            return needConnection() && newConnection();
        } catch (IOException e) {
            System.out.println("[ConnectionHandler/connect] failed by exception :: " + e);
            String message = e.getMessage();
            if (message == null || !message.startsWith("Connection refused")) {
                e.printStackTrace();
            }
        }
        return false;
    }

    protected boolean needConnection() {
        return !writer.sendQueue.isEmpty();
    }

    protected abstract boolean newConnection() throws IOException;

    private synchronized void await(long timeout) {
        try {
            wait(timeout);
        } catch (Exception e) {
            // ignore
        }
    }

    public synchronized void awakeAll() {
        notifyAll();
    }

    public boolean connected(int index, SocketChannel socket) {
        System.out.println("[ConnectionHandler/connected] " + index + "::" + socket);
        return newConnection(socket, false);
    }

    public boolean accepted(int index, SocketChannel socket) {
        System.out.println("[ConnectionHandler/accepted] " + index + "::" + socket);
        return newConnection(socket, false);
    }

    public boolean connected(int index, SocketChannel socket, boolean priority) {
        System.out.println("[ConnectionHandler/connected] " + index + "::" + priority + "::" + socket);
        return newConnection(socket, priority);
    }

    public boolean accepted(int index, SocketChannel socket, boolean priority) {
        System.out.println("[ConnectionHandler/accepted] " + index + "::" + priority + "::" + socket);
        return newConnection(socket, priority);
    }

    public boolean establishing(int index, SocketChannel connected) {
        System.out.println("[ConnectionHandler/established] " + index + "::" + connected);
        return tryEstablish(connected);
    }

    protected synchronized boolean tryEstablish(SocketChannel connected) {
        try {
            return active == connected && (established = true);
        } finally {
            awakeAll();
        }
    }

    protected synchronized boolean newConnection(SocketChannel connected, boolean priority) {
        if (established || !isClosed() && !priority) {
            System.out.println("-- [ConnectionHandler/newConnection] use existing connection " + active + ", close new connection " + connected);
            close(connected);
            return false;
        }
        if (!isClosed() && priority) {
            System.out.println("-- [ConnectionHandler/newConnection] close existing connection " + active + ", use new connection " + connected);
            close(active);
        }
        active = connected;
        return true;
    }

    private void ioFailed(SocketChannel socket, Exception e) {
        if (!shutdowning && !(e instanceof ClosedChannelException)) {
            e.printStackTrace();
        }
        close(socket);
    }

    private synchronized void close(SocketChannel socket) {
        System.out.println("-- [ConnectionHandler/close] " + socket);
        NetUtil.close(socket);
        if (socket == active) {
            established = false;
            active = null;
        }
        awakeAll();
    }

    public synchronized boolean isClosed() {
        return active == null || !active.isConnected();
    }

    protected abstract Message service(Message message);

    private class Reader implements Transceiver {

        ByteBuffer header = ByteBuffer.allocateDirect(Message.HEADER_LENGTH);
        Map<Integer, Request> replyQueue = new ConcurrentHashMap<Integer, Request>();

        public void run() {
            SocketChannel socket;
            while ((socket = available(this)) != null) {
                try {
                    Message message = readMessage(socket);
                    if (message.opcode() == Message.EOF) {
                        System.out.println("-- [Reader/run] EOF from " + socket.socket().getRemoteSocketAddress());
                        close(socket);
                        return;
                    }
                    handleMessage(message);
                } catch (Exception e) {
                    ioFailed(socket, e);
                }
            }
        }

        public void queueWait(int callid, Request request) {
            replyQueue.put(callid, request);
        }

        public void cancelRequest(int callid) {
            if (callid > 0) {
                replyQueue.remove(callid);
            }
        }

        private Message readMessage(SocketChannel socket) throws IOException {
            header.clear();
            NetUtil.readFully(socket, header);
            short magic = header.getShort();
            if (magic != Message.MAGIC) {
                throw new IllegalStateException("invalid magic 0x" + Integer.toHexString(magic));
            }
            byte opcode = header.get();
            byte nspace = header.get();
            int callid = header.getInt();
            int length = header.getInt();

            Message message = createMessage(opcode);
            message.callid = callid;
            message.nspace = nspace;
            System.out.println("-- [Reader/readMessage] " + message + " :: length = " + length);

            return length == 0 ? message : loadMessage(socket, message, length);
        }

        private Message loadMessage(SocketChannel socket, Message message, int length) throws IOException {
            loadObject(socket, message, length);
            if (message instanceof Multiparted) {
                Multiparted multipart = (Multiparted) message;
                int lenth = multipart.hasNext();
                for (; lenth >= 0; lenth = multipart.hasNext()) {
                    loadObject(socket, multipart.next(), lenth);
                }
            }
            return message;
        }

        private <T extends Deserializable> T loadObject(SocketChannel socket, T object, int length) throws IOException {
            int[] index = pool.allocate(length + object.header());
            ByteBuffer buffer = pool.regionFor(index, true);
            
            object.prepare(buffer);
            try {
                NetUtil.readFully(socket, buffer);
                System.out.println(NetUtil.dump(buffer));
                object.deserialize(buffer);
            } catch (IOException e) {
                pool.release(index);
                throw e;
            }
            if (object instanceof Persistable) {
                ((Persistable)object).index(index);
            } else {
                pool.release(index);
            }
            return object;
        }

        private void handleMessage(Message message) {
            if (message.isRequest()) {
                handleRequest(message);
            } else {
                handleReply(message);
            }
        }

        private void handleReply(Message message) {
            int callId = -message.callid;
            boolean aggregation = message instanceof Aggregatable;
            Request request = aggregation ? replyQueue.get(callId) : replyQueue.remove(callId);
            if (request != null) {
                request.arrived(message);
                if (aggregation && ((Aggregatable)message).isFinished(request)) {
                    replyQueue.remove(callId);
                }
            }
        }

        private void handleRequest(Message message) {
            Message reply = service(message);
            if (reply != null && message.callid > 0) {
                reply.callid = -message.callid;
                writer.enqueueMessage(reply);
            }
        }

        public boolean establish(long timeout) {
            long start = System.currentTimeMillis();
            while (timeout > 0 && !established) {
                await(timeout);
                long end = System.currentTimeMillis();
                timeout -= end - start;
                start = end;
            }
            return established;
        }
    }

    private class Writer implements Transceiver {

        final ByteBuffer header = ByteBuffer.allocateDirect(Message.HEADER_LENGTH);

        final List<Message> expireQueue = new LinkedList<Message>();
        final BlockingDeque<Message> sendQueue = new LinkedBlockingDeque<Message>();

        final OutputCollector collector = new OutputCollector(pool);

        public void run() {
            SocketChannel socket;
            while ((socket = available(this)) != null) {
                try {
                    Message message = sendQueue.poll(POLLING, TimeUnit.MILLISECONDS);
                    if (message != null) {
                        enrouteMessage(socket, message);
                    }
                } catch (InterruptedException e) {
                    // ignore
                } catch (Exception e) {
                    ioFailed(socket, e);
                    continue;
                }
                if (shutdowning && sendQueue.isEmpty()) {
                    close(socket);
                }
            }
        }

        public Request request(Message message) {
            Request request = new RequestSingle(resource.nextCallID());
            return request(message, request);
        }

        public Request request(Message message, MessageListener listener) {
            Request request = new ListeningRequest(resource.nextCallID(), listener);
            return request(message, request);
        }

        public Request request(Message message, Request request) {
            message.callid = request.callid();
            reader.queueWait(request.callid(), request);
            enqueueMessage(message);
            return request;
        }

        public void enqueueMessage(Message message) {
            if (message instanceof TransmitExpirable) {
                registerExpirable(message);
            }
            if (message instanceof Immediate) {
                sendQueue.addFirst(message);
                awakeAll();
            } else {
                sendQueue.offer(message);
            }
        }

        private void enrouteMessage(SocketChannel socket, Message message) throws Exception {
            if (message.isCanceled()) {
                reader.cancelRequest(message.callid);
                return;
            }
            boolean expirable = message instanceof TransmitExpirable;
            if (expirable && ((TransmitExpirable) message).isExpired()) {
                return;
            }
            writeMessage(message, socket);
            if (expirable) {
                expireQueue.remove(message);
            }
        }

        private void writeMessage(Message message, SocketChannel socket) throws Exception {
            try {
                message.serialize(collector);
                ByteBuffer[] serialized = message.asHeadered(header, collector);
                System.out.println("-- [Writer/writeMessage] " + message + " :: length = " + collector.length() + ", " + ConnectionHandler.this);
                socket.write(serialized);
            } catch (Exception e) {
                sendQueue.addFirst(message);
                throw e;
            } finally {
                collector.release();
                header.clear();
            }
            message.transmitted();
        }

        private void registerExpirable(Message expirable) {
            synchronized(expireQueue) {
                expireQueue.add(expirable);
            }
        }

        private void checkExpirables() {
            synchronized(expireQueue) {
                Iterator<Message> iterator = expireQueue.iterator();
                while (iterator.hasNext()) {
                    Message message = iterator.next();
                    if (((TransmitExpirable) message).isExpired()) {
                        iterator.remove();
                        sendQueue.remove(message);
                    }
                }
            }
        }

        public boolean establish(long timeout) {
            boolean connected = connect();
            if (!connected && !awaitEstablish(timeout)) {
                checkExpirables();
            }
            return connected;
        }

        private synchronized boolean awaitEstablish(long timeout) {
            if (!established) {
                await(timeout);
            }
            return established;
        }
    }

    private static interface Transceiver extends Runnable {
        boolean establish(long timeout);
    }

    protected abstract Message createMessage(byte opcode);

    public void desynchronized(byte nspace, int partitionID) {
    }
}