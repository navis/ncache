package com.nexr.cache.develop;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ByteBufferPool extends BufferPool {

    ByteBuffer buffer;

    public ByteBufferPool() {}

    public ByteBufferPool(int length, boolean direct) {
        initialize(length, direct);
    }

    public void initialize(int length, boolean direct) {
        buffer = direct ? ByteBuffer.allocateDirect(length) : ByteBuffer.allocate(length);
        put(new int[]{buffer.capacity(), 0});
    }

    public void initialize(ByteBuffer external) {
        buffer = (ByteBuffer) external.clear();
    }

    protected void initialize(int length) {
        initialize(length, true);
    }

    @Override
    public void clear() {
        super.clear();
        buffer.clear();
    }

    @Override
    public synchronized ByteBuffer expand(int delta) {
        return expand(ByteBuffer.allocate(buffer.capacity() + delta));
    }

    @Override
    public ByteBuffer expand(ByteBuffer nbuffer) {
        nbuffer.put((ByteBuffer)buffer.clear()).clear();
        put(index(nbuffer.capacity() - buffer.capacity(), buffer.capacity()));
        return buffer = nbuffer;
    }

    public long capacity() {
        return buffer.capacity();
    }

    protected ByteBuffer failed(int length) {
        return ByteBuffer.allocate(length);     // heap
    }

    protected synchronized ByteBuffer slice(int length, int start) {
        buffer.limit(start + length);
        buffer.position(start);
        return buffer.slice();
    }

    public int[] serialize(byte[] packet) {
        int[] index = allocate(packet.length);
        if (index != null) {
            regionFor(index, true).put(ByteBuffer.wrap(packet));
        }
        return index;
    }

    public ByteBuffer regionFor(int[] index, boolean slice) {
        if (index != null) {
            return slice ? slice(index[0], index[1]) : buffer;
        }
        return null;
    }

    public ByteBuffer regionFor(int[] index, int offset, int length) {
        if (index != null) {
            return slice(index[0] - offset, index[1] + offset);
        }
        return null;
    }

    public int toOffset(int[] index) {
        return index[1];
    }

    public int toLength(int[] index) {
        return index[0];
    }

    public static void main(String[] args) {
        BufferPool pool = new ByteBufferPool();
        pool.initialize(64 << 10);
        
        int[] v1 = pool.allocate(32);
        System.out.println(Arrays.toString(v1));
        System.out.println(pool);
        int[] v2 = pool.allocate(32);
        System.out.println(Arrays.toString(v2));
        System.out.println(pool);
        int[] v3 = pool.allocate(32);
        System.out.println(Arrays.toString(v3));
        System.out.println(pool);
        System.out.println("------------- start release");
        pool.release(v1);
        System.out.println(pool);
        pool.release(v2);
        System.out.println(pool);
        pool.release(v3);
        System.out.println(pool);
        System.out.println("------------- start allocate");
        int[] v4 = pool.allocate(32);
        System.out.println(Arrays.toString(v4));
        System.out.println(pool);
    }
}
