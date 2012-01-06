package com.nexr.cache.message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.nexr.cache.OutputCollector;
import com.nexr.cache.Record;

public abstract class StreamingMessage extends Message implements Multiparted<StreamingMessage.Entry> {

    private int[] lengths;
    private Entry[] entries;

    public Entry[] entries() {
        return entries;
    }

    public void serialize(OutputCollector collector) throws IOException {
        throw new IllegalStateException("serialize");
    }

    public void deserialize(ByteBuffer buffer) throws IOException {
        lengths = new int[buffer.getInt()];
        for (int i = 0; i < lengths.length; i++) {
            lengths[i] = buffer.getInt();
        }
        System.out.println(Arrays.toString(lengths));
        entries = new Entry[lengths.length];
    }

    private transient int index;

    public int hasNext() {
        return index < lengths.length ? lengths[index] : -1;
    }

    public Entry next() {
        return entries[index++] = new Entry();
    }

    public static class Entry extends AbstractDeserializable implements Persistable {

        public int[] mindex;
        public int genHigh;
        public int genLow;
        public int hash;
        public byte flags;

        public void index(int[] index) {
            this.mindex = index;
        }

        public void deserialize(ByteBuffer buffer) throws IOException {
            this.genHigh = buffer.getInt(Record.GSTAMP_HIGH_OFFSET);
            this.genLow = buffer.getInt(Record.GSTAMP_LOW_OFFSET);
            this.hash = buffer.getInt(Record.HASH_OFFSET);
            this.flags = buffer.get(Record.FLAGS_OFFSET);
        }

        public boolean isRemoved() {
            return (flags & Record.FLAG_REMOVED) != 0;
        }

        public boolean isPersist() {
            return (flags & Record.FLAG_PERSIST) != 0;
        }
    }
}
