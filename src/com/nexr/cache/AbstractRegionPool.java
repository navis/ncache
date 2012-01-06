package com.nexr.cache;

public abstract class AbstractRegionPool<T> implements RegionPool<T> {

    public int[] poll(int length) {
        int[] result;
        while ((result = allocate(length)) == null) {
            Thread.yield();
        }
        return result;
    }
}
