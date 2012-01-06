package com.nexr.cache;

import java.nio.ByteBuffer;

public interface MemoryPool extends RegionPool<ByteBuffer> {

    long capacity();

    int[] serialize(byte[] packet);
}
