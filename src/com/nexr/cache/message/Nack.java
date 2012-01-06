package com.nexr.cache.message;

import java.io.IOException;

import com.nexr.cache.OutputCollector;
import com.nexr.cache.ByteBufferInput;

public class Nack extends DataInputMessage {

    String message;

    public Nack() {}

    public Nack(String message) {
        this.message = message;
    }

    public byte opcode() {
        return NACK;
    }

    public void readFields(ByteBufferInput input) throws IOException {
        message = input.readUTF();
    }

    public void serialize(OutputCollector collector) throws IOException {
        OutputCollector.Writer writer = collector.writer(message.length() + 2);
        writer.writeUTF(message);
        writer.collect();
    }

    public String toString() {
        return message == null ? super.toString() : super.toString() + "(" + message + ")";
    }
}
