package com.nexr.cache.develop;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.nexr.cache.develop.Reader;
import com.nexr.cache.Constants;
import com.nexr.cache.Utils;

public class SimpleMClient implements Runnable {

    volatile boolean shutdown = false;

    SocketChannel socket;
    LinkedBlockingQueue<Request> queue;

    Reader reader;

    public SimpleMClient(List<InetSocketAddress> addresses) throws IOException {
        this(addresses.get(0));
    }

    public SimpleMClient(InetSocketAddress address) throws IOException {
        socket = createSocket(address);
        queue = new LinkedBlockingQueue<Request>();
        reader = new Reader(socket, 8 << 10);
        Thread thread = new Thread(this);
        thread.setDaemon(true);
        thread.start();
    }

    private SocketChannel createSocket(InetSocketAddress address) throws IOException {
        SocketChannel socket = SocketChannel.open();
        socket.socket().connect(new InetSocketAddress(address.getAddress(), address.getPort()));
        return socket;
    }

    public boolean set(String key, int flag, int exp, String object) throws IOException {
        Request<Boolean> request = new Set(serializeForSet(key, flag, exp, object));

        queue.add(request);
        return request.waitReply();
    }

    public String get(String key) throws IOException {
        Request<String> request = new Get(serializeForGet(key));
        queue.add(request);
        return request.waitReply();
    }

    private byte[] serializeForGet(String key) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream(key.length() + 32);
        output.write(Constants.GET_);
        output.write(key.getBytes());
        output.write(Constants.NEWLINE);

        return output.toByteArray();
    }

    private byte[] serializeForSet(String key, int flag, int exp, String object) throws IOException {
        byte[] serialized = object.getBytes();

        ByteArrayOutputStream output = new ByteArrayOutputStream(serialized.length + 32);
        output.write(Constants.SET_);
        output.write(key.getBytes());
        output.write(Constants.SPACE);
        output.write(String.valueOf(flag).getBytes());
        output.write(Constants.SPACE);
        output.write(String.valueOf(exp).getBytes());
        output.write(Constants.SPACE);
        output.write(String.valueOf(serialized.length).getBytes());
        output.write(Constants.NEWLINE);
        output.write(serialized);
        output.write(Constants.NEWLINE);

        return output.toByteArray();
    }

    public void run() {
        try {
            while (!shutdown) {
                Request request = queue.take();
                try {
                    socket.write(ByteBuffer.wrap(request.request));

                    request.accept(reader);
                } catch (IOException e) {
                    request.failed(e);
                    throw e;
                }
            }
        } catch (Exception e) {
            if (!shutdown) {
                e.printStackTrace();
            }
        } finally {
            Utils.close(socket);
        }
    }

    public void shutdown() {
        shutdown = true;
        Utils.close(socket);
    }

    private static abstract class Request<T> {

        byte[] request;

        boolean responded;
        T respond;
        IOException failed;

        public Request(byte[] packet) {
            this.request = packet;
        }

        protected abstract T convert(Reader reader) throws IOException;

        public synchronized T waitReply() throws IOException {
            while (!responded) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new InterruptedIOException(e.getMessage());
                }
            }
            if (failed != null) {
                throw failed;
            }
            return respond;
        }

        public synchronized void accept(Reader reader) throws IOException {
            responded = true;
            respond = convert(reader);
            notifyAll();
        }

        public synchronized void failed(IOException ex) throws IOException {
            responded = true;
            failed = ex;
            notifyAll();
        }
    }

    private static class Set extends Request<Boolean> {

        public Set(byte[] packet) {
            super(packet);
        }

        protected Boolean convert(Reader reader) throws IOException {
            byte[][] command = reader.splitCommand();
            return Arrays.equals(command[0], Constants.STORED);
        }
    }

    private static class Get extends Request<String> {

        public Get(byte[] packet) {
            super(packet);
        }

        protected String convert(Reader reader) throws IOException {

            List<String> result = new ArrayList<String>();

            while (true) {
                byte[][] command = reader.splitCommand();
                if (Arrays.equals(command[0], Constants.END)) {
                    return result.isEmpty() ? null : result.get(0);
                }
                if (!Arrays.equals(command[0], Constants.VALUE)) {
                    throw new IOException(new String(command[0]));
                }
                int flag = Utils.parseInt(command[2]);
                int length = Utils.parseInt(command[3]);
                reader.next();

                command = reader.splitCommand();
                result.add(new String(command[0]));
                reader.next();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        SimpleMClient client = new SimpleMClient(new InetSocketAddress("localhost", 9876));
        System.out.println(client.get("navis"));

        System.out.println(client.set("navis", 1, 0, "navis-value"));
        System.out.println(client.get("navis"));
    }
}
