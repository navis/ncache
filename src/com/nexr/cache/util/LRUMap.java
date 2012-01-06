package com.nexr.cache.util;

import java.util.ArrayList;
import java.util.List;

public abstract class LRUMap<K, V> extends UsageAwareMap<K, V> {

    long maximum;

    public LRUMap(long maximum) {
        super(true);
        this.maximum = maximum;
    }

    public List<V> clearAll() {
        List<V> copy = new ArrayList<V>(values());
        super.clear();
        return copy;
    }

    @Override
    protected boolean removeEldestEntry(Entry<K, V> eldest) {
        return occupied > maximum;
    }
}
