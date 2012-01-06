package com.nexr.cache.message;

import java.io.IOException;

import com.nexr.cache.ByteBufferInput;
import com.nexr.cache.OutputCollector;
import com.nexr.cache.ServerEntity;

public class RemoveForwarded extends DataInputMessage implements SourceAware {

    public byte[] rkey;
    public int[] rinformation;

    public transient int hash;
    private transient ServerEntity source;

    public RemoveForwarded() {}

    public RemoveForwarded(byte[] key, int[] information, byte nspace) {
        this.rkey = key;
        this.rinformation = information;
        this.nspace = nspace;
    }

    public byte opcode() {
        return FWD_REMOVE;
    }

    @Override
    public void readFields(ByteBufferInput input) throws IOException {
        rkey = input.readKey();
        rinformation = new int[]{input.readInt(), input.readInt(), hash = input.readInt()};
    }

    public void serialize(OutputCollector collector) throws IOException {
        throw new IllegalStateException("serialize");
    }

    public int index() {
        return source == null ? -1 : source.index();
    }

    public ServerEntity source() {
        return source;
    }

    public void source(ServerEntity source) {
        this.source = source;
    }
}

