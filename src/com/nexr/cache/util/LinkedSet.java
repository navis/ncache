package com.nexr.cache.util;

import java.util.*;

@SuppressWarnings("unchecked")
public class LinkedSet<V> implements Iterable<V> {

    static final int DEFAULT_INITIAL_CAPACITY = 16;
    static final int MAXIMUM_CAPACITY = 1 << 30;
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    protected transient Entry[] table;
    protected transient int size;

    protected transient Entry<V> cHeader;
    protected transient Entry<V> lruHeader;

    protected int threshold;
    protected final float loadFactor;

    public LinkedSet(int initialCapacity, float loadFactor) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Illegal initial capacity: " +
                    initialCapacity);
        }
        if (initialCapacity > MAXIMUM_CAPACITY) {
            initialCapacity = MAXIMUM_CAPACITY;
        }
        if (loadFactor <= 0 || Float.isNaN(loadFactor)) {
            throw new IllegalArgumentException("Illegal load factor: " +
                    loadFactor);
        }

        // Find a power of 2 >= initialCapacity
        int capacity = 1;
        while (capacity < initialCapacity) {
            capacity <<= 1;
        }

        this.loadFactor = loadFactor;
        this.threshold = (int) (capacity * loadFactor);
        this.table = new Entry[capacity];
        initialize();
    }

    public LinkedSet(int initialCapacity, boolean accessOrder) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    public LinkedSet(boolean accessOrder) {
        this.loadFactor = DEFAULT_LOAD_FACTOR;
        this.threshold = (int) (DEFAULT_INITIAL_CAPACITY * DEFAULT_LOAD_FACTOR);
        table = new Entry[DEFAULT_INITIAL_CAPACITY];
        initialize();
    }

    protected void initialize() {
        lruHeader = cHeader = newEntry(-1, null);
        lruHeader.before = lruHeader.after = lruHeader;
        cHeader.cbefore = cHeader.cafter = cHeader;
    }

    protected Entry LRC() {
        return cHeader.cafter;
    }

    protected Entry LRU() {
        return lruHeader.cafter;
    }

    private int rehash(int h) {
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }

    protected int hashFor(V value) {
        return value.hashCode();
    }

    protected boolean lookup(Object key, V stored) {
        return equals((V)key, stored);
    }

    protected boolean equals(V key, V stored) {
        return key.equals(stored);
    }

    protected int indexFor(int h, int length) {
        return h & (length - 1);
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public V get(V key) {
        return get(key, hashFor(key));
    }

    public V get(Object key, int hash) {
        return getHash(key, rehash(hash), false);
    }

    protected final synchronized V getHash(Object key, int hash, boolean revise) {
        int index = indexFor(hash, table.length);
        for (Entry<V> e = table[index]; e != null; e = e.next) {
            if (e.hash == hash && (e.value == key || lookup(key, e.value))) {
                return accessed(e);
            }
        }
        return null;
    }

    public Entry<V> put(V value) {
        return put(value, hashFor(value));
    }

    public Entry<V> put(V value, int hash) {
        return putHash(value, rehash(hash));
    }

    protected final synchronized Entry<V> putHash(V value, int hash) {
        int bucket = indexFor(hash, table.length);
        for (Entry<V> e = table[bucket]; e != null; e = e.next) {
            if (e.hash == hash && (e.value == value || equals(value, e.value))) {
                changing(e, value);
                return e;
            }
        }
        Entry<V> appended = appendEntry(hash, value, bucket);
        appended(appended);
        return null;
    }

    public Entry<V> put(V value, ConflictResolver<V> resolver) {
        return put(value, hashFor(value), resolver);
    }

    public Entry<V> put(V value, int hash, ConflictResolver<V> resolver) {
        return putHash(value, rehash(hash), resolver);
    }

    protected final synchronized Entry<V> putHash(V value, int hash, ConflictResolver<V> resolver) {
        int i = indexFor(hash, table.length);
        for (Entry<V> e = table[i]; e != null; e = e.next) {
            if (e.hash == hash && (e.value == value || equals(value, e.value))) {
                if (!resolver.override(value, e.value)) {
                    return null;
                }
                changing(e, value);
                return e;
            }
        }
        Entry<V> appended = appendEntry(hash, value, i);
        appended(appended);
        return null;
    }

    private void resize(int newCapacity) {
        if (table.length == MAXIMUM_CAPACITY) {
            threshold = Integer.MAX_VALUE;
            return;
        }
        table = transferTo(newCapacity);
        threshold = (int) (newCapacity * loadFactor);
    }

    private Entry[] transferTo(int capacity) {
        Entry[] newTable = new Entry[capacity];
        for (Entry<V> e = lruHeader.after; e != lruHeader; e = e.after) {
            int index = indexFor(e.hash, capacity);
            e.next = newTable[index];
            newTable[index] = e;
        }
        return newTable;
    }

    public Entry<V> remove(V key) {
        return remove(key, hashFor(key));
    }

    public Entry<V> remove(Object key, int hash) {
        return removeHash(key, rehash(hash));
    }

    protected final synchronized Entry<V> removeHash(Object key, int hash) {
        int i = indexFor(hash, table.length);
        Entry<V> prev = table[i];
        Entry<V> e = prev;

        while (e != null) {
            Entry<V> next = e.next;

            if (e.hash == hash && (e.value == key || lookup(key, e.value))) {
                size--;
                if (prev == e) {
                    table[i] = next;
                } else {
                    prev.next = next;
                }
                removing(e);
                return e;
            }
            prev = e;
            e = next;
        }
        return null;
    }

    public Entry<V> removeMark(V key) {
        return removeMark(key, hashFor(key));
    }

    public Entry<V> removeMark(Object key, int hash) {
        return removeMarkHash(key, rehash(hash));
    }

    protected final synchronized Entry<V> removeMarkHash(Object key, int hash) {
        int index = indexFor(hash, table.length);
        for (Entry<V> e = table[index]; e != null; e = e.next) {
            if (e.hash == hash && (e.value == key || lookup(key, e.value))) {
                marking(e);
                return e;
            }
        }
        return null;
    }


    public <E> Entry<V> remove(E expected, V key, ConflictResolver<E> resolver) {
        return remove(expected, key, hashFor(key), resolver);
    }

    public <E> Entry<V> remove(E expected, Object key, int hash, ConflictResolver<E> resolver) {
        return removeHash(expected, key, rehash(hash), resolver);
    }

    protected final synchronized <E> Entry<V> removeHash(E expected, Object key, int hash, ConflictResolver<E> resolver) {
        int i = indexFor(hash, table.length);
        Entry<V> prev = table[i];
        Entry<V> e = prev;

        while (e != null) {
            Entry<V> next = e.next;

            if (e.hash == hash && (e.value == key || lookup(key, e.value))) {
                if (!resolver.override(expected, resolve(expected, e.value))) {
                    return null;
                }
                size--;
                if (prev == e) {
                    table[i] = next;
                } else {
                    prev.next = next;
                }
                removing(e);
                return e;
            }
            prev = e;
            e = next;
        }
        return null;
    }

    public <E> Entry<V> removeMark(E expected, V key, ConflictResolver<E> resolver) {
        return removeMark(expected, key, hashFor(key), resolver);
    }

    public <E> Entry<V> removeMark(E expected, Object key, int hash, ConflictResolver<E> resolver) {
        return removeMarkHash(expected, key, rehash(hash), resolver);
    }

    protected final synchronized <E> Entry<V> removeMarkHash(E expected, Object key, int hash, ConflictResolver<E> resolver) {
        int index = indexFor(hash, table.length);
        for (Entry<V> e = table[index]; e != null; e = e.next) {
            if (e.hash == hash && (e.value == key || lookup(key, e.value))) {
                if (!resolver.override(expected, resolve(expected, e.value))) {
                    return null;
                }
                marking(e);
                return e;
            }
        }
        return null;
    }

    public synchronized void clear() {
        clearHash();
        clearList();
        size = 0;
    }

    private void clearHash() {
        Arrays.fill(table, null);
    }

    private void clearList() {
        lruHeader.before = lruHeader.after = lruHeader;
        cHeader.cbefore = cHeader.cafter = cHeader;
    }

    public Iterator<V> iterator() {
        return new LRUValueIterator();
    }

    public static class Entry<K> {

        int hash;
        K value;

        Entry<K> next;      // map
        Entry<K> before;    // lru list
        Entry<K> after;     // lru list

        Entry<K> cbefore;    // create list
        Entry<K> cafter;     // create list

        Entry(int hash, K value) {
            this.hash = hash;
            this.value = value;
        }

        public K value() {
            return value;
        }

        public K newValue(K newValue) {
            K oldValue = value;
            value = newValue;
            return oldValue;
        }

        private void remove() {
            removeC();
            removeLRU();
        }

        private void removeC() {
            cbefore.cafter = cafter;
            cafter.cbefore = cbefore;
        }

        private void removeLRU() {
            before.after = after;
            after.before = before;
        }

        private void delink() {
            delinkC();
            delinkLRU();
            next = null;
        }

        private void delinkC() {
            cbefore = cafter = null;
        }

        private void delinkLRU() {
            before = after = null;
        }

        private void appendC(Entry<K> cPrev) {
            newC(cPrev);
        }

        private void appendLRU(Entry<K> lruPrev) {
            newLRU(lruPrev);
        }

        private void append(Entry<K> cPrev, Entry<K> lruPrev) {
            newC(cPrev);
            newLRU(lruPrev);
        }

        private void newC(Entry<K> cPrev) {
            cafter = cPrev;
            cbefore = cPrev.cbefore;
            cbefore.cafter = this;
            cafter.cbefore = this;
        }

        private void newLRU(Entry<K> lruPrev) {
            after = lruPrev;
            before = lruPrev.before;
            before.after = this;
            after.before = this;
        }

        public final String toString() {
            return value.toString();
        }
    }

    protected void appended(Entry<V> appended) {
        if (++size >= threshold) {
            resize(table.length << 1);
        }
    }

    protected V accessed(Entry<V> target) {
        target.removeLRU();
        target.appendLRU(lruHeader);
        return target.value();
    }

    protected V changing(Entry<V> target, V nvalue) {
        V ovalue = target.newValue(nvalue);
        changed(target);
        return ovalue;
    }

    protected void changed(Entry<V> target) {
        target.remove();
        target.append(cHeader, lruHeader);
    }

    protected void marking(Entry<V> target) {
        target.removeC();
        target.appendC(cHeader);
    }

    protected void removing(Entry<V> target) {
        target.remove();
        target.delink();
    }

    /**
     * Like addEntry except that this version is used when creating entries
     * as part of Map construction or "pseudo-construction" (cloning,
     * deserialization).  This version needn't worry about resizing the table.
     * <p/>
     * Subclass overrides this to alter the behavior of HashMap(Map),
     * clone, and readObject.
     */
    Entry<V> appendEntry(int hash, V value, int bucket) {
        Entry<V> entry = newEntry(hash, value);
        entry.next = table[bucket];
        table[bucket] = entry;
        entry.append(cHeader, lruHeader);
        return entry;
    }

    protected Entry<V> newEntry(int hash, V value) {
        return new Entry<V>(hash, value);
    }

    public final class LRUEntryIterator extends LRUIterator<Entry<V>> {
        public Entry<V> next() {
            return nextEntry();
        }
    }

    public final class CEntryIterator extends CIterator<Entry<V>> {
        public Entry<V> next() {
            return nextEntry();
        }
    }

    public final class LRUValueIterator extends LRUIterator<V> {
        public V next() {
            return nextEntry().value();
        }
    }

    public final class CValueIterator extends CIterator<V> {
        public V next() {
            return nextEntry().value();
        }
    }

    public final class CValueIteratorR extends CIteratorR<V> {
        public V next() {
            return nextEntry().value();
        }
    }

    // Views
    private transient Set<Entry<V>> entrySet;
    private transient Collection<V> values;
    private transient Collection<V> cvalues;
    private transient Collection<Entry<V>> centries;

    public synchronized Collection<V> values() {
        return values != null ? values : (values = new Values());
    }

    public synchronized Collection<V> cvalues() {
        return cvalues != null ? cvalues : (cvalues = new Cvalues());
    }

    public synchronized Collection<Entry<V>> centries() {
        return centries != null ? centries : (centries = new Centries());
    }

    private final class Values extends AbstractCollection<V> {

        public Iterator<V> iterator() {
            return new LRUValueIterator();
        }

        public int size() {
            return size;
        }
    }

    private final class Cvalues extends AbstractCollection<V> {

        public Iterator<V> iterator() {
            return new CValueIterator();
        }

        public int size() {
            return size;
        }
    }

    private final class Centries extends AbstractCollection<Entry<V>> {

        public Iterator<Entry<V>> iterator() {
            return new CEntryIterator();
        }

        public int size() {
            return size;
        }
    }

    public synchronized Set<Entry<V>> entrySet() {
        return entrySet != null ? entrySet : (entrySet = new EntrySet());
    }

    private final class EntrySet extends AbstractSet<Entry<V>> {
        public Iterator<Entry<V>> iterator() {
            return new LRUEntryIterator();
        }
        public int size() {
            return size;
        }
    }

    private abstract class LRUIterator<T> implements Iterator<T> {

        Entry<V> nextEntry = lruHeader.after;
        Entry<V> lastReturned;

        public boolean hasNext() {
            return nextEntry != lruHeader;
        }

        public void remove() {
            if (lastReturned == null) {
                throw new IllegalStateException();
            }
            LinkedSet.this.remove(lastReturned.value);
            lastReturned = null;
        }

        Entry<V> nextEntry() {
            if (nextEntry == lruHeader) {
                throw new NoSuchElementException();
            }
            Entry<V> e = lastReturned = nextEntry;
            nextEntry = e.after;
            return e;
        }
    }

    private abstract class CIterator<T> implements Iterator<T> {

        Entry<V> nextEntry = cHeader.cafter;
        Entry<V> lastReturned;

        public boolean hasNext() {
            return nextEntry != cHeader;
        }

        public void remove() {
            if (lastReturned == null) {
                throw new IllegalStateException();
            }
            LinkedSet.this.remove(lastReturned.value);
            lastReturned = null;
        }

        Entry<V> nextEntry() {
            if (nextEntry == cHeader) {
                throw new NoSuchElementException();
            }
            Entry<V> e = lastReturned = nextEntry;
            nextEntry = e.cafter;
            return e;
        }
    }

    private abstract class CIteratorR<T> implements Iterator<T> {

        Entry<V> nextEntry = cHeader.cbefore;
        Entry<V> lastReturned;

        public boolean hasNext() {
            return nextEntry != cHeader;
        }

        public void remove() {
            if (lastReturned == null) {
                throw new IllegalStateException();
            }
            LinkedSet.this.remove(lastReturned.value);
            lastReturned = null;
        }

        Entry<V> nextEntry() {
            if (nextEntry == cHeader) {
                throw new NoSuchElementException();
            }
            Entry<V> e = lastReturned = nextEntry;
            nextEntry = e.cbefore;
            return e;
        }
    }

    @Deprecated
    public synchronized void rehashAll() {
        clearHash();
        LRUEntryIterator iterator = new LRUEntryIterator();
        while (iterator.hasNext()) {
            Entry<V> entry = iterator.next();
            entry.hash = hashFor(entry.value);
            int index = indexFor(entry.hash, table.length);
            entry.next = table[index];
            table[index] = entry;
        }
    }

    protected boolean removeEldestEntry(Entry<V> eldest) {
        return false;
    }

    public static interface ConflictResolver<C> {
        boolean override(C expected, C current);
    }
}
