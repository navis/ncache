package com.nexr.cache.client;

import java.nio.ByteBuffer;

import com.nexr.cache.MemoryPool;

public class DummyPool implements MemoryPool {

    public long capacity() {
        return Long.MAX_VALUE;
    }

    public int[] allocate(int length) {
        return new int[]{0, length};
    }

    public int[] poll(int length) {
        return new int[]{0, length};
    }

    public int[] serialize(byte[] packet) {
        throw new UnsupportedOperationException("serialize");
    }

    public void release(int[] cached) {
    }

    public ByteBuffer regionFor(int[] index, boolean slice) {
        return ByteBuffer.allocate(index[1]);
    }

    public ByteBuffer regionFor(int[] index, int offset, int length) {
        return ByteBuffer.allocate(index[1]);
    }

    public int toOffset(int[] index) {
        return index[0];
    }

    public int toLength(int[] index) {
        return index[1];
    }

    public String toString(int[] index) {
        return String.valueOf(index[1]);
    }
}
