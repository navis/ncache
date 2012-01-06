package com.nexr.cache.message;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.nexr.cache.OutputCollector;

public class Ack extends Message {

    public Ack() {}

    public Ack(int callid) {
        this.callid = callid;
    }

    public byte opcode() {
        return ACK;
    }
    public void serialize(OutputCollector collector) throws IOException { }
    public void deserialize(ByteBuffer buffer) throws IOException { }
}
