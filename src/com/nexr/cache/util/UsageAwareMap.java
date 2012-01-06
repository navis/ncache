package com.nexr.cache.util;

import java.util.Map;

public abstract class UsageAwareMap<K, V> extends LinkedHashMap<K, V> {

    long occupied;

    public UsageAwareMap(boolean accessOrder) {
        super(accessOrder);
    }

    @Override
    protected void appended(Map.Entry<K, V> appended) {
        occupied += toLength(appended);
        super.appended(appended);
    }

    @Override
    protected void removed(Map.Entry<K, V> entry) {
        occupied -= toLength(entry);
        super.removed(entry);
    }

    @Override
    protected void changed(Map.Entry<K, V> entry, V prev) {
        occupied -= toLength(entry.getKey(), prev);
        occupied += toLength(entry);
        super.changed(entry, prev);
    }

    protected int toLength(Map.Entry<K, V> entry) {
        return toLength(entry.getKey(), entry.getValue());
    }

    protected String toString(Map.Entry<K, V> entry) {
        return toString(entry.getKey(), entry.getValue());
    }

    @Override
    public void clear() {
        super.clear();
        occupied = 0;
    }

    protected abstract int toLength(K k, V v);

    protected abstract String toString(K k, V v);
}
