package com.nexr.cache.util;

import java.util.Iterator;

public abstract class UsageAwareSet<V> extends LinkedSet<V> {

    protected Entry<V> cursor;
    protected long occupied;

    public UsageAwareSet(boolean accessOrder) {
        super(accessOrder);
        cursor = lruHeader;
    }

    @Override
    protected void appended(Entry<V> appended) {
        occupied += toLength(appended.value());
        super.appended(appended);
    }

    @Override
    protected void removing(Entry<V> entry) {
        if (cursor == entry) {
            cursor = entry.after;
        }
        occupied -= toLength(entry.value());
        super.removing(entry);
    }

    @Override
    protected V changing(Entry<V> entry, V nvalue) {
        occupied -= toLength(entry.value());
        occupied += toLength(nvalue);
        return super.changing(entry, nvalue);
    }

    @Override
    public void clear() {
        super.clear();
        occupied = 0;
    }

    protected abstract int toLength(V v);

    protected abstract String toString(V v);

    public Iterator<V> cursor() {
        return new Cursor();
    }

    private class Cursor implements Iterator<V> {

        private Cursor() {
            if (cursor.after == lruHeader) {
                cursor = lruHeader;
            }
        }

        public boolean hasNext() {
            return cursor.after != lruHeader;
        }

        public V next() {
            cursor = cursor.after;
            return cursor.value();
        }

        public void remove() {
            throw new UnsupportedOperationException("remove");
        }
    }
}
