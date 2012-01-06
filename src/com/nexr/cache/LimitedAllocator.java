package com.nexr.cache;

import java.util.concurrent.atomic.AtomicLong;
import java.nio.ByteBuffer;

public class LimitedAllocator implements Region.Allocator {

    long limit;
    AtomicLong current;

    public LimitedAllocator(long limit) {
        this.limit = limit;
        this.current = new AtomicLong();
    }

    public Region region(int sindex, int length) {
        if (current.addAndGet(length) > limit) {
            current.addAndGet(-length);
            return null;
        }
        ByteBuffer region = ByteBuffer.allocateDirect(length);
        return new Region<ByteBuffer>(region, sindex, region.capacity());
    }

    public long capacity() {
        return limit;
    }
}
