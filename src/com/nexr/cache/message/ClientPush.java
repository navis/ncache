package com.nexr.cache.message;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.nexr.cache.OutputCollector;
import com.nexr.cache.Record;
import com.nexr.cache.Utils;

public class ClientPush extends Message implements TransmitExpirable {

    public int expire;
    public boolean persist;

    public int hash;
    public byte[] key;
    public byte[] value;

    transient long ttl;

    public ClientPush(byte[] key, byte[] value, int expire, boolean persist, byte nspace, long timeout) {
        this.key = key;
        this.hash = Utils.hash(key);
        this.value = value;
        this.expire = expire;
        this.persist = persist;
        this.nspace = nspace;
        this.ttl = Utils.afterMillis(timeout);
    }

    public byte opcode() {
        return REQ_STORE;
    }

    public void deserialize(ByteBuffer buffer) throws IOException {
        throw new UnsupportedOperationException("deserialize");
    }

    public void serialize(OutputCollector collector) throws IOException {
        OutputCollector.Writer writer = collector.writer(Record.CLIENT_HEADER_LEN + key.length + value.length);
        writer.writeInt(hash);
        writer.writeInt(expire);
        writer.writeBoolean(persist);

        writer.writeKey(key);
        writer.write(value);
        writer.collect();
    }

    public boolean isExpired() {
        return ttl < Utils.current();
    }
}
