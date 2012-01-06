package com.nexr.cache.message;

import java.io.IOException;

import com.nexr.cache.ByteBufferInput;
import com.nexr.cache.OutputCollector;

public class Flush extends DataInputMessage {
    public byte opcode() {
        return Message.REQ_FLUSH;
    }

    public void readFields(ByteBufferInput input) throws IOException {
    }

    public void serialize(OutputCollector collector) throws IOException {
    }
}
