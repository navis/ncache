package com.nexr.cache.message;

import java.io.IOException;

import com.nexr.cache.ByteBufferInput;
import com.nexr.cache.OutputCollector;

public class ClientRetrieve extends DataInputMessage {

    public byte[] value;

    public byte opcode() {
        return RETRIEVED;
    }

    public void readFields(ByteBufferInput input) throws IOException {
        value = input.readAll();
    }

    public void serialize(OutputCollector collector) throws IOException {
        throw new UnsupportedOperationException("write");
    }
}
