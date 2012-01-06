package com.nexr.cache.message;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.nexr.cache.OutputCollector;
import com.nexr.cache.Region;

public class SyncData extends Message {

    int[][] mindexes;

    public SyncData(byte namespace, int[][] mindexes) {
        this.nspace = namespace;
        this.mindexes = mindexes;
    }

    public byte opcode() {
        return SYNC_DATA;
    }

    public void serialize(OutputCollector collector) throws IOException {
        ByteBuffer buffer = collector.temporary(4 + mindexes.length * 4);
        buffer.putInt(mindexes.length);
        for (int[] mindex : mindexes) {
            buffer.putInt(Region.toLength(mindex));
        }
        buffer.flip();
    }

    @Override
    protected ByteBuffer[] asPackaged(ByteBuffer header, OutputCollector collector) {
        for (int[] mindex : mindexes) {
            collector.collect(mindex);
        }
        return super.asPackaged(header, collector);
    }

    public void deserialize(ByteBuffer buffer) throws IOException {
        throw new IllegalStateException("deserialize");
    }
}
