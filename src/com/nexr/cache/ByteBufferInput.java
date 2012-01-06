package com.nexr.cache;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.nexr.cache.util.NetUtil;

public class ByteBufferInput extends DataInputStream implements DataInput {

    ByteBuffer buffer;

    public ByteBufferInput(ByteBuffer buffer) {
        super(NetUtil.newInputStream(buffer));
        this.buffer = buffer;
    }

    public byte[] readKey() throws IOException {
        byte[] key = new byte[readShort()];
        readFully(key);
        return key;
    }

    public byte[] readBytes(int length) throws IOException {
        byte[] value = new byte[length];
        readFully(value);
        return value;
    }

    public byte[] readAll() throws IOException {
        byte[] value = new byte[available()];
        readFully(value);
        return value;
    }

    public int position() {
        return buffer.position();
    }

    public ByteBuffer buffer() {
        return buffer;
    }
}
