package com.nexr.cache.message;

import java.nio.ByteBuffer;
import java.io.IOException;

import com.nexr.cache.OutputCollector;

public class SyncCompleted extends Message {

    int partition;

    public SyncCompleted() {
    }

    public SyncCompleted(byte namespace, int partition) {
        this.nspace = namespace;
        this.partition = partition;
    }

    public int partition() {
        return partition;
    }

    public byte opcode() {
        return SYNC_COMPLETED;
    }

    public void deserialize(ByteBuffer buffer) throws IOException {
        partition = buffer.getInt();
    }

    public void serialize(OutputCollector collector) throws IOException {
        ByteBuffer buffer = collector.temporary(4);
        buffer.putInt(partition);
        buffer.flip();
    }
}
