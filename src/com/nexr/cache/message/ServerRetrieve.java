package com.nexr.cache.message;

import java.io.IOException;

import com.nexr.cache.ByteBufferInput;
import com.nexr.cache.OutputCollector;
import com.nexr.cache.Record;

public class ServerRetrieve extends DataInputMessage {

    private int keylen;
    private int[] value;

    public ServerRetrieve(int keylen, int[] value) {
        this.keylen = keylen;
        this.value = value;
    }

    public byte opcode() {
        return RETRIEVED;
    }

    @Override
    public void readFields(ByteBufferInput input) throws IOException {
        throw new IllegalStateException("read");
    }

    @Override
    public void serialize(OutputCollector collector) throws IOException {
        collector.collect(value, Record.KEY_OFFSET + keylen, -1);
    }
}
