package com.nexr.cache;

import java.nio.ByteBuffer;

public class PooledAllocator implements Region.Allocator<ByteBuffer> {

    ByteBuffer pooled;

    public PooledAllocator(int length, boolean direct) {
        pooled = direct ? ByteBuffer.allocateDirect(length) : ByteBuffer.allocate(length);
        pooled.position(0).limit(0);
    }

    public long capacity() {
        return pooled.capacity();
    }

    public synchronized Region<ByteBuffer> region(int sindex, int length) {
        int limit = pooled.limit();
        if (pooled.capacity() - limit < length) {
            return null;
        }
        pooled.limit(limit + length).position(limit);
        
        ByteBuffer sliced = pooled.slice();
        return new Region<ByteBuffer>(sliced, sindex, sliced.capacity());
    }
}
