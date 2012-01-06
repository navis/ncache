package com.nexr.cache.message;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.nexr.cache.OutputCollector;

public class Desync extends Message {

    public Entry[] entries;

    public Desync(Entry[] entries) {
        this.entries = entries;
    }

    public byte opcode() {
        return CONFLICTED;
    }

    public void deserialize(ByteBuffer buffer) throws IOException {
        entries = new Entry[buffer.getInt()];
        for (int i = 0; i < entries.length; i++) {
            entries[i] = new Entry(buffer.get(), buffer.getInt());
        }
    }

    public void serialize(OutputCollector collector) throws IOException {
        ByteBuffer buffer = collector.temporary(4 + 5 * entries.length);
        buffer.putInt(entries.length);
        for (Entry entry : entries) {
            buffer.put(entry.nspace);
            buffer.putInt(entry.partition);
        }
    }

    public static class Entry {

        public byte nspace;
        public int partition;

        public Entry(byte nspace, int partitionID) {
            this.nspace = nspace;
            this.partition = partitionID;
        }

        public int hashCode() {
            return partition + nspace << 24;
        }

        public boolean equals(Object obj) {
            Entry other = (Entry) obj;
            return nspace == other.nspace && partition == other.partition;
        }
    }
}
