package com.nexr.cache;

import java.nio.ByteBuffer;

import com.nexr.cache.Region.Allocator;
import com.nexr.cache.client.SimpleAllocator;

public class MemcachedMPool extends MemcachedRegionPool<ByteBuffer> implements MemoryPool {

    Allocator<ByteBuffer> allocator = new SimpleAllocator();

    protected RegionRack<ByteBuffer> newRack(int slabSize) {
        return new MemoryRack(slabsize);
    }

    public int[] serialize(byte[] packet) {
        int[] index = allocate(packet.length);
        if (index != null) {
            regionFor(index, true).put(ByteBuffer.wrap(packet));
        }
        return index;
    }

    public long capacity() {
        return allocator.capacity();
    }

    private class MemoryRack extends RegionRack<ByteBuffer> {

        public MemoryRack(int slabsize) {
            super(slabsize);
        }

        @Override
        protected ByteBuffer slice(ByteBuffer buffer, int[] index, int coffset, int clength) {
            int offset = Region.toOffset(index);
            if (clength < 0) {
                int length = Region.toLength(index);
                return slice(buffer, offset + length, offset + coffset);
            }
            return slice(buffer, offset + coffset + clength, offset + coffset);
        }

        protected ByteBuffer slice(ByteBuffer buffer, int end, int start) {
            synchronized (buffer) {
                buffer.limit(end);
                buffer.position(start);
                ByteBuffer sliced = buffer.slice();
                buffer.clear();
                return sliced;
            }
        }

        protected Region<ByteBuffer> region(int sindex, int rlength, int slabsize) {
            return allocator.region(sindex, Math.max(rlength, slabsize));
        }
    }
}
