package com.nexr.cache.util;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

@SuppressWarnings("unchecked")
public class LinkedHashMap<K, V> extends HashMap<K, V> implements Iterable<V> {

    private transient Entry<K, V> header;

    private final boolean accessOrder;

    public LinkedHashMap(boolean accessOrder) {
        this.accessOrder = accessOrder;
    }

    public LinkedHashMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
        accessOrder = false;
    }

    protected void initialize() {
        header = new Entry<K, V>(-1, null, null, null);
        header.before = header.after = header;
    }

    void transfer(HashMap.Entry[] newTable) {
        int newCapacity = newTable.length;
        for (Entry<K, V> e = header.after; e != header; e = e.after) {
            int index = indexFor(e.hash, newCapacity);
            e.next = newTable[index];
            newTable[index] = e;
        }
    }

    @Override
    public void clear() {
        super.clear();
        header.before = header.after = header;
    }

    @Override
    protected void appended(Map.Entry<K, V> appended) {
        Entry<K, V> eldest = header.after;
        while (eldest != header) {
            if (!removeEldestEntry(eldest)) {
                break;
            }
            K ekey = eldest.key;
            eldest = eldest.after;
            remove(ekey);
        }
        super.appended(appended);
    }

    @Override
    protected void accessed(Map.Entry<K, V> target) {
        Entry<K, V> entry = (Entry<K,V>) target;
        if (accessOrder) {
            modCount++;
            entry.remove();
            entry.addBefore(header);
        }
    }

    @Override
    protected void changed(Map.Entry<K, V> target, V prev) {
        accessed(target);
    }

    @Override
    protected void removed(Map.Entry<K, V> target) {
        Entry<K, V> entry = (Entry<K,V>) target;
        entry.remove();
    }

    public Iterator<V> iterator() {
        return newValueIterator();
    }

    public static class Entry<K, V> extends HashMap.Entry<K, V> {
        // These fields comprise the doubly linked list used for iteration.
        Entry<K, V> before;
        Entry<K, V> after;

        Entry(int hash, K key, V value, HashMap.Entry<K, V> next) {
            super(hash, key, value, next);
        }

        private void remove() {
            before.after = after;
            after.before = before;
        }

        private void addBefore(Entry<K, V> existingEntry) {
            after = existingEntry;
            before = existingEntry.before;
            before.after = this;
            after.before = this;
        }
    }

    private abstract class LinkedHashIterator<T> implements Iterator<T> {

        Entry<K, V> nextEntry = header.after;
        Entry<K, V> lastReturned;

        /**
         * The modCount value that the iterator believes that the backing
         * List should have.  If this expectation is violated, the iterator
         * has detected concurrent modification.
         */
        int expectedModCount = modCount;

        public boolean hasNext() {
            return nextEntry != header;
        }

        public void remove() {
            if (lastReturned == null) {
                throw new IllegalStateException();
            }
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }

            LinkedHashMap.this.remove(lastReturned.key);
            lastReturned = null;
            expectedModCount = modCount;
        }

        Entry<K, V> nextEntry() {
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
            if (nextEntry == header) {
                throw new NoSuchElementException();
            }

            Entry<K, V> e = lastReturned = nextEntry;
            nextEntry = e.after;
            return e;
        }
    }

    private class KeyIterator extends LinkedHashIterator<K> {
        public K next() { return nextEntry().getKey(); }
    }

    private class ValueIterator extends LinkedHashIterator<V> {
        public V next() { return nextEntry().value; }
    }

    private class EntryIterator extends LinkedHashIterator<Map.Entry<K, V>> {
        public Map.Entry<K, V> next() { return nextEntry(); }
    }

    Iterator<K> newKeyIterator() { return new KeyIterator(); }

    Iterator<V> newValueIterator() { return new ValueIterator(); }

    Iterator<Map.Entry<K, V>> newEntryIterator() { return new EntryIterator(); }

    @Override
    protected Entry<K, V> appendEntry(int hash, K key, V value, int bucketIndex) {
        Entry<K, V> e = (Entry<K, V>) super.appendEntry(hash, key, value, bucketIndex);
        e.addBefore(header);
        return e;
    }

    @Override
    protected Entry<K, V> newEntry(int hash, K key, V value, int bucketIndex) {
        HashMap.Entry<K, V> old = table[bucketIndex];
        return new Entry<K, V>(hash, key, value, old);
    }

    protected boolean removeEldestEntry(Entry<K, V> eldest) {
        return false;
    }
}

