package com.nexr.cache.message;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.nexr.cache.ByteBufferInput;

public abstract class DataInputMessage extends Message {

    public void deserialize(ByteBuffer buffer) throws IOException {
        readFields(new ByteBufferInput(buffer));
    }

    protected void readFields(ByteBufferInput input) throws IOException {}

    public String toString() {
        return super.toString() + "[" + callid + "]";
    }
}
