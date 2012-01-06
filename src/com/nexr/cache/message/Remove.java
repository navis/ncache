package com.nexr.cache.message;

import java.io.IOException;

import com.nexr.cache.OutputCollector;
import com.nexr.cache.ByteBufferInput;
import com.nexr.cache.Utils;
import com.nexr.cache.ServerEntity;

public class Remove extends DataInputMessage {

    public byte[] key;
    public int hash;

    public Remove() {}

    public Remove(byte[] key, byte nspace) {
        this(key, Utils.hash(key), nspace);
    }

    public Remove(byte[] key, int hash, byte nspace) {
        this.key = key;
        this.hash = Utils.hash(key);
    }

    public byte opcode() {
        return REQ_REMOVE;
    }

    @Override
    public void readFields(ByteBufferInput input) throws IOException {
        key = input.readKey();
        hash = input.readInt();
    }

    public void serialize(OutputCollector collector) throws IOException {
        OutputCollector.Writer writer = collector.writer(key.length + 6);
        writer.writeKey(key);
        writer.writeInt(hash);
        writer.collect();
    }
}
