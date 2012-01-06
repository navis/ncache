package com.nexr.cache.util;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class NetUtil {

    public static InetSocketAddress toAddress(byte[] value) throws IOException {
        if (value == null) {return null;}
        return address(new DataInputStream(new ByteArrayInputStream(value)).readUTF());
    }

    public static InetSocketAddress address(String address) {
        int index = address.lastIndexOf(':');
        if (index < 0) {
            return new InetSocketAddress("127.0.0.1", Integer.parseInt(address));
        }
        return new InetSocketAddress(address.substring(0, index), Integer.parseInt(address.substring(index + 1)));
    }

    public static ServerSocketChannel serverChannel(String address) throws IOException {
        return serverChannel(address(address));
    }

    public static ServerSocketChannel serverChannel(InetSocketAddress address) throws IOException {
        ServerSocketChannel ssocket = ServerSocketChannel.open();
        ssocket.socket().setPerformancePreferences(0, 2, 1);
        ssocket.socket().setReceiveBufferSize(16 << 10);
        ssocket.socket().setReuseAddress(true);

        ssocket.socket().bind(address);
        return ssocket;
    }

    public static SocketChannel newChannel(String address) throws IOException {
        SocketChannel socket = SocketChannel.open();
        socket.socket().setPerformancePreferences(0, 2, 1);
        socket.socket().setReceiveBufferSize(16 << 10);
        socket.socket().setTrafficClass(0x10);          // IPTOS_LOWDELAY

        socket.socket().connect(address(address));

        socket.socket().setSoLinger(false, 0);
        socket.socket().setTcpNoDelay(true);
        return socket;
    }

    public static DataInput newDataStream(ByteBuffer buf) {
        return new DataInputStream(newInputStream(buf));
    }

    public static InputStream newInputStream(final ByteBuffer buf) {
        return new InputStream() {
            public synchronized int read() throws IOException {
                return buf.hasRemaining() ? buf.get() & 0xff : -1;
            }

            @Override
            public synchronized int read(byte[] bytes, int off, int len) throws IOException {
                len = Math.min(len, available());
                buf.get(bytes, off, len);
                return len;
            }

            @Override
            public int available() throws IOException {
                return buf.remaining();
            }
        };
    }

    public static long readFully(SocketChannel socket, ByteBuffer... buffers) throws IOException {
        long total = 0;
        while (buffers[buffers.length - 1].hasRemaining()) {
            long read = socket.read(buffers);
            if (read < 0) {
                throw new EOFException();
            }
            total += read;
        }
        for (ByteBuffer buffer : buffers) {
            buffer.flip();
        }
        return total;
    }

    public static int readFully(SocketChannel socket, ByteBuffer buffer) throws IOException {
        int total = 0;
        while (buffer.hasRemaining()) {
            int read = socket.read(buffer);
            if (read < 0) {
                throw new EOFException();
            }
            total += read;
        }
        buffer.flip();
//        System.out.println(dump(buffer));
        return total;
    }

    public static String dump(ByteBuffer buffer) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < buffer.remaining(); i++) {
            int value = buffer.get(i) & 0xff;
            if (value < 0x10) {
                builder.append('0');
            }
            builder.append(Integer.toHexString(value));
            if (i < buffer.remaining() - 1) {
                builder.append(' ');
            }
        }
        return builder.toString();
    }

    public static void close(SocketChannel socket) {
        try {
            socket.close();
        } catch (Exception e) {
            // ignore
        }
    }

    public static void close(ServerSocketChannel socket) {
        try {
            socket.close();
        } catch (Exception e) {
            // ignore
        }
    }
}
