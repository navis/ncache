package com.nexr.cache;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class OutputCollector {

    MemoryPool pool;

    int index;
    ByteBuffer[] outputs;
    List<int[]> temps;

    public OutputCollector(MemoryPool pool) {
        this.pool = pool;
        this.temps = new ArrayList<int[]>();
        this.outputs = new ByteBuffer[10];
    }

    public Writer writer(int length) {
        return new Writer(length);
    }

    public int length() {
        int length = 0;
        for (int i = 0; i < index; i++) {
            length += outputs[i].remaining();
        }
        return length;
    }

    public ByteBuffer temporary(int length) {
        int[] index = pool.allocate(length);
        ByteBuffer buffer = pool.regionFor(temporary(index), true);
        return output(buffer);
    }

    private int[] temporary(int[] index) {
        temps.add(index);
        return index;
    }

    private ByteBuffer output(ByteBuffer buffer) {
        if (index >= outputs.length) {
            ByteBuffer[] array = new ByteBuffer[outputs.length << 1];
            System.arraycopy(outputs, 0, array, 0, index);
            outputs = array;
        }
        return outputs[index++] = buffer;
    }

    public void collect(byte[] packet) {
        int[] index = pool.allocate(packet.length);
        ByteBuffer buffer = pool.regionFor(temporary(index), true);
        buffer.put(ByteBuffer.wrap(packet)).flip();
        output(buffer);
    }

    public void collect(int[] index) {
        output(pool.regionFor(index, true));
    }

    public void collect(int[] index, int offset, int length) {
        output(pool.regionFor(index, offset, length));
    }

    public void collect(ByteBuffer buffer) {
        output(buffer);
    }

    public ByteBuffer[] collected(ByteBuffer header) {
        ByteBuffer[] array = new ByteBuffer[index + 1];
        System.arraycopy(outputs, 0, array, 1, index);
        array[0] = header; 
        return array;
    }

    public void release() {
        for (int[] temp : temps) {
            pool.release(temp);
        }
        temps.clear();
        index = 0;
    }

    public ByteBuffer bufferFor(int[] index) {
        return pool.regionFor(index, true);
    }

    public class Writer extends DataOutputStream {

        public Writer(int length) {
            super(new ByteArrayOutputStream(length));
        }

        public void collect() {
            OutputCollector.this.collect(((ByteArrayOutputStream) out).toByteArray());
        }

        public void writeKey(byte[] key) throws IOException {
            writeShort(key.length);
            write(key);
        }
    }
}
