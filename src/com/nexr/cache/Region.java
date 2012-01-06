package com.nexr.cache;

public class Region<T> {

    public static final int SLAB_INDEX = 0;
    public static final int OFFSET_INDEX = 1;
    public static final int LENGTH_INDEX = 2;

    T region;
    int[] index;

    public Region(T region, int[] index) {
        this.region = region;
        this.index = index;
    }

    public Region(T region, int sindex, int length) {
        this.region = region;
        this.index = index(sindex, 0, length);
    }

    public static int[] index(int source, int offset, int length) {
        return new int[] {source, offset, length};
    }

    public static int toSlabs(int[] index) {
        return index[SLAB_INDEX];
    }

    public static int toOffset(int[] index) {
        return index[OFFSET_INDEX];
    }

    public static int toLength(int[] index) {
        return index[LENGTH_INDEX];
    }

    public static boolean isSame(int[] index1, int[] index2) {
        return index1[SLAB_INDEX] == index2[SLAB_INDEX] && index1[OFFSET_INDEX] == index2[OFFSET_INDEX];
    }

    public static String toString(int[] index) {
        return index[SLAB_INDEX] +":" + index[OFFSET_INDEX] + ":" + index[LENGTH_INDEX];
    }

    public static interface Allocator<T> {
        Region<T> region(int source, int length);
        long capacity();
    }
}
