package com.nexr.cache.util;

import java.util.List;
import java.util.ArrayList;

public abstract class LRUSet<V> extends UsageAwareSet<V> {

    protected long maximum;

    public LRUSet(long maximum) {
        super(true);
        this.maximum = maximum;
    }

    public List<V> clearAll() {
        List<V> copy = new ArrayList<V>(values());
        super.clear();
        return copy;
    }

    @Override
    protected void appended(Entry<V> appended) {
        Entry<V> eldest = lruHeader.after;
        while (eldest != lruHeader) {
            if (!removeEldestEntry(eldest)) {
                break;
            }
            Entry<V> after = eldest.after;
            removeHash(eldest.value, eldest.hash);
            eldest = after;
        }
        super.appended(appended);
    }

    @Override
    protected boolean removeEldestEntry(Entry<V> eldest) {
        return occupied > maximum;
    }

    public int usageKB() {
        return (int) (occupied >> 10);
    }

    public int remainKB() {
        return (int) ((maximum - occupied) >> 10);
    }
}
