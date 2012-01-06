package com.nexr.cache.message;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.nexr.cache.OutputCollector;

public abstract class Forward extends Message implements TransmitExpirable {

    private int[] mindex;

    public Forward(int[] mindex, byte nspace) {
        this.mindex = mindex;
        this.nspace = nspace;
    }

    public byte opcode() {
        return FWD_STORE;
    }

    public void serialize(OutputCollector collector) throws IOException {
        collector.collect(mindex);
    }

    public void deserialize(ByteBuffer buffer) throws IOException {
        throw new IllegalStateException("deserialize");
    }
}
