package com.nexr.cache.message;

import java.io.IOException;

import com.nexr.cache.ByteBufferInput;
import com.nexr.cache.OutputCollector;
import com.nexr.cache.Utils;

public class Get extends DataInputMessage implements TransmitExpirable {

    public byte[] key;
    public int hash;

    transient long ttl;

    public Get() {
    }
    
    public Get(byte[] key, byte nspace, long timeout) {
        this.key = key;
        this.hash = Utils.hash(key);
        this.nspace = nspace;
        this.ttl = Utils.afterMillis(timeout);
    }

    public byte opcode() {
        return REQ_GET;
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

    public boolean isExpired() {
        return ttl < Utils.current();
    }
}
