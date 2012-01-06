package com.nexr.cache.develop;

public class Buffer<T> {

    final int[] index;      // length, offset
    final T buffer;

    public Buffer(int[] index, T buffer) {
        this.index = index;
        this.buffer = buffer;
    }

    public boolean cached() {
        return index != null;
    }

    public int length() {
        return index[0];
    }

    public int offset() {
        return index[1];
    }

    public String toString() {
        return index[0] + ":" + index[1];
    }
}
