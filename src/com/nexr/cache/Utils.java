package com.nexr.cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.BitSet;

public class Utils {

    private static final int MULTMIN_RADIX_TEN = Integer.MIN_VALUE / 10;
    private static final int N_MULTMAX_RADIX_TEN = -Integer.MAX_VALUE / 10;

    public static int parseInt(byte[] s) throws NumberFormatException {
        return parseInt(s, 0, s.length);
    }

    public static int parseInt(byte[] s, int start, int end) throws NumberFormatException {
        boolean negative = s[start] == '-';

        int limit = negative ? Integer.MIN_VALUE : -Integer.MAX_VALUE;
        int multmin = negative ? MULTMIN_RADIX_TEN : N_MULTMAX_RADIX_TEN;

        int index = negative ? start + 1 : start;

        int result = 0;
        while (index < end) {
            // Accumulating negatively avoids surprises near MAX_VALUE
            int digit = Character.digit(s[index++], 10);
            if (digit < 0) {
                throw new NumberFormatException(new String(s));
            }
            if (result < multmin) {
                throw new NumberFormatException(new String(s));
            }
            result *= 10;
            if (result < limit + digit) {
                throw new NumberFormatException(new String(s));
            }
            result -= digit;
        }

        return negative ? result : -result;
    }

    public static int hash(byte[] value) {
        return hash(value, 0, value.length);
    }

    public static int hash(byte[] value, int offset, int length) {
        int h = 0;
        for (int i = 0; i < length; i++) {
            h = 31 * h + value[offset++];
        }
        return h;
    }

    public static int hash(ByteBuffer value, int offset, int length) {
        int h = 0;
        for (int i = 0; i < length; i++) {
            h = 31 * h + value.get(offset++);
        }
        return h;
    }

    public static int seek(byte[] value, int offset, byte seek) {
        for (int i = offset; i < value.length; i++) {
            if (value[i] == seek) {
                return i;
            }
        }
        return -1;
    }

    public static boolean equals(byte[] value, int offset, int limit, byte[] to) {
        if (limit - offset != to.length) {
            return false;
        }
        for (int i = offset, j = 0; i < limit; i++, j++) {
            if (value[i] != to[j]) {
                return false;
            }
        }
        return true;
    }

    public static String toString(byte[][] command) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < command.length; i++) {
            builder.append(new String(command[i]));
            if (i < command.length - 1) {
                builder.append(',');
            }
        }
        return builder.toString();
    }

    public static void close(SocketChannel socket) {
        try {
            socket.close();
        } catch (IOException e) {
            // ignore
        }
    }

    public static String keyString(ByteBuffer region) {
        byte[] kbuf = new byte[region.getShort()];
        region.get(kbuf);
        return new String(kbuf);
    }

    public static String valueString(ByteBuffer region) {
        short klen = region.getShort();
        byte[] vbuf = new byte[region.capacity() - klen - 2];
        synchronized(region) {
            region.position(klen + 2);
            region.get(vbuf);
            region.clear();
        }
        return new String(vbuf);
    }

    public static String kvString(ByteBuffer region, byte delimiter) {
        short klen = region.getShort();
        int remain = region.remaining();
        byte[] kvbuf = new byte[klen == remain ? remain : remain + 1];
        region.get(kvbuf, 0, klen);
        if (klen < remain) {
            kvbuf[klen] = delimiter;
            region.get(kvbuf, klen + 1, kvbuf.length - klen - 1);
        }
        return new String(kvbuf);
    }

    public static long current() {
        return System.currentTimeMillis();
    }

    public static int currentSeconds() {
        return (int)(System.currentTimeMillis() / 1000);
    }

    public static int afterSeconds(int second) {
        return currentSeconds() + second;
    }

    public static long afterMillis(long expire) {
        return current()  + expire;
    }

    public static String lastPath(String path) {
        int index = path.lastIndexOf('/');
        if (index < 0) {
            throw new IllegalStateException();
        }
        return path.endsWith("/") ? path.substring(index + 1, path.length() - 1) : path.substring(index + 1);
    }

    public static String syncStatus(int length, BitSet status) {
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append(status.get(i) ? 'o' : 'x');
        }
        return builder.toString();
    }

    public static void main(String[] args) {
        System.out.println(System.nanoTime());
        System.out.println(System.nanoTime());
        System.out.println(System.nanoTime());
        System.out.println(System.nanoTime());
    }
}
