package com.nexr.cache.message;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.nexr.cache.OutputCollector;
import com.nexr.cache.Record;
import com.nexr.cache.ServerEntity;

public class StoreForwarded extends Message implements Persistable, SourceAware {

    public transient int[] index;

    public transient int genHigh;
    public transient int genLow;
    public transient int hash;
    public transient byte flags;

    private transient ServerEntity source;

    public StoreForwarded() {}

    public byte opcode() {
        return FWD_STORE;
    }

    public void serialize(OutputCollector collector) throws IOException {
        throw new IllegalStateException("serialize");
    }

    public void deserialize(ByteBuffer buffer) throws IOException {
        this.genHigh = buffer.getInt(Record.GSTAMP_OFFSET);
        this.genLow = buffer.getInt(Record.GSTAMP_OFFSET + 4);
        this.hash = buffer.getInt(Record.HASH_OFFSET);
        this.flags = buffer.get(Record.FLAGS_OFFSET);
    }

    public void index(int[] index) {
        this.index = index;
    }

    public boolean isPersist() {
        return (flags & Record.FLAG_PERSIST) != 0;
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
