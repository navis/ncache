package com.nexr.cache.message;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.nexr.cache.Constants;
import com.nexr.cache.OutputCollector;
import com.nexr.cache.Record;
import com.nexr.cache.Utils;
import com.nexr.cache.util.Generations;

public class ClientPushed extends Message implements Persistable {

    public int[] index;
    public ByteBuffer buffer;

    public transient long generation;

    public transient int hash;
    public transient int expire;
    public transient byte flags;

    public byte opcode() {
        return Message.REQ_STORE;
    }

    public void serialize(OutputCollector collector) throws IOException {
        throw new IllegalStateException("serialize");
    }

    @Override
    public int header() {
        return Record.GSTAMP_LENTH;
    }

    public void deserialize(ByteBuffer buffer) throws IOException {
        this.buffer = buffer;
        this.hash = buffer.getInt(Record.HASH_OFFSET);
        this.expire = buffer.getInt(Record.EXPIRE_OFFSET);
        this.flags = buffer.get(Record.FLAGS_OFFSET);
    }

    public void index(int[] index) {
        this.index = index;
    }

    public void reviseData(int server, long counter) {
        generation = Generations.NEW(server, counter);
        buffer.putLong(Record.GSTAMP_OFFSET, generation);
        if (expire > 0 && expire < Constants.E30DAYS) {
            buffer.putInt(Record.EXPIRE_OFFSET, expire = Utils.afterSeconds(expire));
        }
    }

    public boolean isPersist() {
        return (flags & Record.FLAG_PERSIST) != 0;
    }
}
