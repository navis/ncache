package com.nexr.cache;

public interface RegionPool<T> {

    int[] allocate(int length);

    int[] poll(int length);

    void release(int[] cached);

    T regionFor(int[] index, boolean slice);

    T regionFor(int[] index, int coffset, int clength);
}
