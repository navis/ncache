package com.nexr.cache.client;

import java.nio.ByteBuffer;

import com.nexr.cache.Region;

public class SimpleAllocator implements Region.Allocator<ByteBuffer> {

    public Region<ByteBuffer> region(int sindex, int length) {
        ByteBuffer region = ByteBuffer.allocateDirect(length);
        return new Region<ByteBuffer>(region, sindex, region.capacity());
    }

    public long capacity() {
        return Long.MAX_VALUE;
    }
}
