package com.nexr.cache.util;

public class InherentPriorityQueue<T extends Comparable<T>> extends PriorityQueue<T> {

    public InherentPriorityQueue(int maxSize, boolean ascending) {
        super(maxSize, ascending);
    }

    protected boolean lessThan(T a, T b) {
        return ascending ? a.compareTo(b) < 0 : a.compareTo(b) > 0;
    }
}
