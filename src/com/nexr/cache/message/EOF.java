package com.nexr.cache.message;

import java.io.IOException;

import com.nexr.cache.ByteBufferInput;
import com.nexr.cache.OutputCollector;

public class EOF extends DataInputMessage {

    public byte opcode() {
        return EOF;
    }

    public void readFields(ByteBufferInput input) throws IOException {
    }

    public void serialize(OutputCollector collector) throws IOException {
    }
}
