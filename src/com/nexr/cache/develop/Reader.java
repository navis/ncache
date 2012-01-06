package com.nexr.cache.develop;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Reader {

    int[] counter = new int[2];
    SocketChannel socket;
    ByteBuffer buffer;

    public Reader(SocketChannel socket, int length) {
        this.socket = socket;
        this.buffer = ByteBuffer.allocateDirect(length);
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    public byte[][] splitCommand() throws IOException {
        List<byte[]> command = new ArrayList<byte[]>();
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        boolean r = false;
        for (; readMore(); counter[0]++) {
            byte value = buffer.get(counter[0]);
            if (value == ' ') {
                if (output.size() > 0) {
                    command.add(output.toByteArray());
                    output.reset();
                }
                r = false;
                continue;
            }
            if (value == '\r') {
                if (r) {
                    output.write('\r');
                }
                r = true;
                continue;
            }
            if (value == '\n') {
                if (!r) {
                    output.write('\n');
                    continue;
                }
                if (output.size() > 0) {
                    command.add(output.toByteArray());
                }
                counter[0]++;
                break;
            }
            output.write(value);
            r = false;
        }
        buffer.flip().position(counter[0]);
        return command.toArray(new byte[command.size()][]);
    }

    private boolean readMore() throws IOException {
        if (counter[0] < counter[1]) {
            return true;
        }
        buffer.clear();
        counter[0] = 0;
        counter[1] = socket.read(buffer);
        if (counter[1] < 0) {
            throw new EOFException();
        }
        return true;
    }

    public void next() {
        counter[0] = buffer.position();
        counter[1] = buffer.limit();
        buffer.position(buffer.limit());
        buffer.limit(buffer.capacity());
    }
}
