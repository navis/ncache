package com.nexr.cache.develop;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import com.nexr.cache.*;
import com.nexr.cache.util.NetUtil;

public class MemcachedTextServer implements Runnable {

    public static final ByteBuffer END_D = ByteBuffer.allocateDirect(Constants.END_N.length);

    static {
        END_D.put(ByteBuffer.wrap(Constants.END_N)).flip();
    }

    MapElement cache;
    MemoryPool pool;
    InetSocketAddress socketAddr;

    public MemcachedTextServer(String address, MemoryPool pool) {
        this.socketAddr = NetUtil.address(address);
        this.cache = new MapElement();
        this.pool = pool;
    }

    public void start(boolean block) throws InterruptedException {
        Thread runner = new Thread(this);
        runner.setDaemon(true);
        runner.start();

        if (block) {
            runner.join();
        }
    }

    public void run() {
        try {
            ServerSocketChannel ssocket = ServerSocketChannel.open();
            ssocket.socket().bind(socketAddr);
            while (true) {
                SocketChannel socket = ssocket.accept();
                socket.socket().setTcpNoDelay(true);
                socket.socket().setPerformancePreferences(0, 2, 1);
                socket.socket().setTrafficClass(0x10);
                Worker worker = new Worker(socket);
                new Thread(worker).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public class Worker implements Runnable {

        Reader reader;
        SocketChannel socket;

        OutputCollector collector;

        Worker(SocketChannel socket) throws IOException {
            this.socket = socket;
            this.reader = new Reader(socket, 8 << 10);
            this.collector = new OutputCollector(pool);
        }

        public void run() {
            try {
                while (true) {
                    executeCommand();
                }
            } catch (EOFException e) {
                // ignore
            } catch (IOException e) {
                if (!"Connection reset by peer".equals(e.getMessage())) {
                    e.printStackTrace();
                }
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }

        private void executeCommand() throws IOException {
            byte[][] command = reader.splitCommand();
            if (command.length == 0) {
                return;
            }
//            System.out.println("[SimpleServer$Worker/executeCommand] " + Utils.toString(command) + " " + reader.buffer());
            if (Arrays.equals(command[0], Constants.GET) || Arrays.equals(command[0], Constants.GETS)) {
                executeGet(command);
            } else if (Arrays.equals(command[0], Constants.SET)) {
                executeSet(command);
            } else if (Arrays.equals(command[0], Constants.FLUSH_ALL)) {
                flushAll(command);
            } else {
                throw new IOException(Utils.toString(command));
            }
            reader.next();
        }

        private void executeGet(byte[][] command) throws IOException {
            try {
                for (int i = 1; i < command.length; i++) {
                    byte[] key = command[i];
                    MapElement.Value value = cache.retrieve(key);
                    if (value != null) {
                        int vlength = Region.toLength(value.vindex);
                        collector.collect(prefix(key, new byte[]{0, 0, 0, 0}, vlength - 2));
                        collector.collect(pool.regionFor(value.vindex, true));
                    }

                }
                collector.collect(END_D.duplicate());
                socket.write(collector.collected());
            } finally {
                collector.release();
            }
        }

        private byte[] prefix(byte[] bkey, byte[] flag, int length) throws IOException {
            ByteArrayOutputStream bout = new ByteArrayOutputStream(32);
            bout.write(Constants.VALUE_);
            bout.write(bkey);
            bout.write(Constants.SPACE);
            bout.write(flag);
            bout.write(Constants.SPACE);
            bout.write(String.valueOf(length).getBytes());
            bout.write(Constants.NEWLINE);
            return bout.toByteArray();
        }

        private void executeSet(byte[][] command) throws IOException {
            byte[] key = command[1];
            byte[] flag = command[2];
            int expire = Utils.parseInt(command[3]);
            int length = Utils.parseInt(command[4]);

            boolean success = store(key, flag, expire, length);
            socket.write(ByteBuffer.wrap(success ? Constants.STORED_N : Constants.NOT_STORED_N));
        }

        private boolean store(byte[] key, byte[] flag, int expire, int length) throws IOException {
            int[] vindex = pool.allocate(length + Constants.NEWLINE.length);
            if (vindex == null) {
                return false;
            }
            ByteBuffer buffer = pool.regionFor(vindex, true);
            buffer.put(reader.buffer());
            try {
                while (buffer.remaining() > 0) {
                    int read = socket.read(buffer);
                    if (read < 0) {
                        throw new EOFException("failed to read " + length + ", remain " + buffer.remaining());
                    }
                }
            } catch (IOException e) {
                pool.release(vindex);
                throw e;
            }
            MapElement.Value prev = cache.store(key, expire, vindex);
            if (prev != null) {
                pool.release(prev.vindex);
            }
            return true;
        }

        private void flushAll(byte[][] command) throws IOException {
            cache.clear();
            for (MapElement.Value index : cache.clear().values()) {
                pool.release(index.vindex);
            }

            socket.write(ByteBuffer.wrap(Constants.OK));
        }
    }
}