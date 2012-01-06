package com.nexr.cache;

import java.nio.ByteBuffer;

public abstract class AbstractMemoryPool implements MemoryPool {

    public int[] poll(int length) {
        int[] result;
        while ((result = allocate(length)) == null) {
            Thread.yield();
        }
        return result;
    }

    public int[] serialize(byte[] packet) {
        int[] index = allocate(packet.length);
        if (index != null) {
            regionFor(index, true).put(ByteBuffer.wrap(packet));
        }
        return index;
    }
}
