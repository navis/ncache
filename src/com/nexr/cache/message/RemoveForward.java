package com.nexr.cache.message;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.nexr.cache.ByteBufferInput;
import com.nexr.cache.OutputCollector;

public abstract class RemoveForward extends Message implements TransmitExpirable {

    public byte[] rkey;
    public int[] rinformation;

    public transient int hash;

    public RemoveForward() {}

    public RemoveForward(byte[] key, int[] information, byte nspace) {
        this.rkey = key;
        this.rinformation = information;
        this.nspace = nspace;
    }

    public byte opcode() {
        return FWD_REMOVE;
    }

    public void deserialize(ByteBuffer buffer) throws IOException {
        throw new IllegalStateException("deserialize");
    }

    public void serialize(OutputCollector collector) throws IOException {
        OutputCollector.Writer writer = collector.writer(rkey.length + 14);
        writer.writeKey(rkey);
        writer.writeInt(rinformation[0]);
        writer.writeInt(rinformation[1]);
        writer.writeInt(rinformation[2]);
        writer.collect();
    }
}
