package com.nexr.cache.util;

import java.util.*;

@SuppressWarnings("unchecked")
public class HashMap<K, V> {

    static final int DEFAULT_INITIAL_CAPACITY = 16;
    static final int MAXIMUM_CAPACITY = 1 << 30;
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    transient Entry[] table;
    transient int size;

    int threshold;
    final float loadFactor;

    transient volatile int modCount;

    public HashMap(int initialCapacity, float loadFactor) {
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
        threshold = (int) (capacity * loadFactor);
        table = new Entry[capacity];
        initialize();
    }

    public HashMap(int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    public HashMap() {
        this.loadFactor = DEFAULT_LOAD_FACTOR;
        threshold = (int) (DEFAULT_INITIAL_CAPACITY * DEFAULT_LOAD_FACTOR);
        table = new Entry[DEFAULT_INITIAL_CAPACITY];
        initialize();
    }

    protected void initialize() {
    }

    private int rehash(int h) {
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }

    protected int hashFor(K key) {
        return key.hashCode();
    }

    protected boolean lookup(Object key, K stored) {
        return equals((K) key, stored);
    }

    protected boolean equals(K key, K stored) {
        return key.equals(stored);
    }

    static int indexFor(int h, int length) {
        return h & (length - 1);
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     * <p/>
     * <p>More formally, if this map contains a mapping from a key
     * {@code k} to a value {@code v} such that {@code (key==null ? k==null :
     * key.equals(k))}, then this method returns {@code v}; otherwise
     * it returns {@code null}.  (There can be at most one such mapping.)
     * <p/>
     * <p>A return value of {@code null} does not <i>necessarily</i>
     * indicate that the map contains no mapping for the key; it's also
     * possible that the map explicitly maps the key to {@code null}.
     * The {@link #containsKey containsKey} operation may be used to
     * distinguish these two cases.
     *
     * @see #put(Object,Object)
     */
    public V get(K key) {
        return get(key, hashFor(key));
    }

    public V get(Object key, int hash) {
        return getHash(key, rehash(hash));
    }

    private V getHash(Object key, int hash) {
        int index = indexFor(hash, table.length);
        for (Entry<K, V> e = table[index]; e != null; e = e.next) {
            if (e.hash == hash && (e.key == key || lookup(key, e.key))) {
                accessed(e);
                return e.value;
            }
        }
        return null;
    }

    public V put(K key, V value) {
        return put(key, hashFor(key), value);
    }

    public V put(K key, int hash, V value) {
        return putHash(key, rehash(hash), value);
    }

    private V putHash(K key, int hash, V value) {
        int i = indexFor(hash, table.length);
        for (Entry<K, V> e = table[i]; e != null; e = e.next) {
            if (e.hash == hash && (e.key == key || equals(key, e.key))) {
                V oldValue = e.set(key, value);
                changed(e, oldValue);
                return oldValue;
            }
        }
        modCount++;

        Entry<K, V> appended = appendEntry(hash, key, value, i);
        appended(appended);
        return null;
    }

    void resize(int newCapacity) {
        Entry[] oldTable = table;
        int oldCapacity = oldTable.length;
        if (oldCapacity == MAXIMUM_CAPACITY) {
            threshold = Integer.MAX_VALUE;
            return;
        }

        Entry[] newTable = new Entry[newCapacity];
        transfer(newTable);
        table = newTable;
        threshold = (int) (newCapacity * loadFactor);
    }

    void transfer(Entry[] newTable) {
        Entry[] src = table;
        int newCapacity = newTable.length;
        for (int j = 0; j < src.length; j++) {
            Entry<K, V> e = src[j];
            if (e != null) {
                src[j] = null;
                do {
                    Entry<K, V> next = e.next;
                    int i = indexFor(e.hash, newCapacity);
                    e.next = newTable[i];
                    newTable[i] = e;
                    e = next;
                } while (e != null);
            }
        }
    }

    public V remove(K key) {
        return remove(key, hashFor(key));
    }

    public V remove(Object key, int hash) {
        Entry<K, V> entry = removeHash(key, rehash(hash));
        return entry == null ? null : entry.value;
    }

    public Entry<K, V> removeEntry(K key) {
        return removeEntry(key, hashFor(key));
    }

    public Entry<K, V> removeEntry(Object key, int hash) {
        return removeHash(key, rehash(hash));
    }

    private Entry<K, V> removeHash(Object key, int hash) {
        int i = indexFor(hash, table.length);
        Entry<K, V> prev = table[i];
        Entry<K, V> e = prev;

        while (e != null) {
            Entry<K, V> next = e.next;

            if (e.hash == hash && (e.key == key || lookup(key, e.key))) {
                modCount++;
                size--;
                if (prev == e) {
                    table[i] = next;
                } else {
                    prev.next = next;
                }
                removed(e);
                return e;
            }
            prev = e;
            e = next;
        }

        return null;
    }

    public void clear() {
        modCount++;
        Entry[] tab = table;
        for (int i = 0; i < tab.length; i++) {
            tab[i] = null;
        }
        size = 0;
    }

    static class Entry<K, V> implements Map.Entry<K, V> {
        int hash;
        K key;
        V value;
        Entry<K, V> next;

        Entry(int hash, K key, V value, Entry<K, V> next) {
            this.hash = hash;
            this.key = key;
            this.value = value;
            this.next = next;
        }

        public final K getKey() {
            return key;
        }

        public final V getValue() {
            return value;
        }

        public final V set(K key, V value) {
            V oldValue = this.value;
            this.key = key;
            this.value = value;
            return oldValue;
        }

        public final V setValue(V newValue) {
            V oldValue = value;
            value = newValue;
            return oldValue;
        }

        public final String toString() {
            return getKey() + "=" + getValue();
        }
    }

    protected void appended(Map.Entry<K, V> appended) {
        if (++size >= threshold) {
            resize(2 * table.length);
        }
    }

    protected void accessed(Map.Entry<K, V> target) {
    }

    protected void removed(Map.Entry<K, V> target) {
    }

    protected void changed(Map.Entry<K, V> target, V prev) {
    }

    /**
     * Like addEntry except that this version is used when creating entries
     * as part of Map construction or "pseudo-construction" (cloning,
     * deserialization).  This version needn't worry about resizing the table.
     * <p/>
     * Subclass overrides this to alter the behavior of HashMap(Map),
     * clone, and readObject.
     */
    Entry<K, V> appendEntry(int hash, K key, V value, int bucketIndex) {
        return table[bucketIndex] = newEntry(hash, key, value, bucketIndex);
    }

    protected Entry<K, V> newEntry(int hash, K key, V value, int bucketIndex) {
        Entry<K, V> e = table[bucketIndex];
        return new Entry<K, V>(hash, key, value, e);
    }

    private abstract class HashIterator<E> implements Iterator<E> {
        Entry<K, V> next;    // next entry to return
        int expectedModCount;    // For fast-fail
        int index;        // current slot
        Entry<K, V> current;    // current entry

        HashIterator() {
            expectedModCount = modCount;
            if (size > 0) { // advance to first entry
                Entry[] t = table;
                while (index < t.length && (next = t[index++]) == null) {
                }
            }
        }

        public final boolean hasNext() {
            return next != null;
        }

        final Entry<K, V> nextEntry() {
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
            Entry<K, V> e = next;
            if (e == null) {
                throw new NoSuchElementException();
            }

            if ((next = e.next) == null) {
                Entry[] t = table;
                while (index < t.length && (next = t[index++]) == null) {
                }
            }
            current = e;
            return e;
        }

        public void remove() {
            throw new UnsupportedOperationException("remove");
        }
    }

    private final class ValueIterator extends HashIterator<V> {
        public V next() {
            return nextEntry().value;
        }
    }

    private final class KeyIterator extends HashIterator<K> {
        public K next() {
            return nextEntry().getKey();
        }
    }

    private final class EntryIterator extends HashIterator<Map.Entry<K, V>> {
        public Map.Entry<K, V> next() {
            return nextEntry();
        }
    }

    // Subclass overrides these to alter behavior of views' iterator() method
    Iterator<K> newKeyIterator() {
        return new KeyIterator();
    }

    Iterator<V> newValueIterator() {
        return new ValueIterator();
    }

    Iterator<Map.Entry<K, V>> newEntryIterator() {
        return new EntryIterator();
    }

    // Views

    private transient Set<Map.Entry<K, V>> entrySet;
    private transient volatile Collection<V> values;

    /**
     * Returns a {@link java.util.Collection} view of the values contained in this map.
     * The collection is backed by the map, so changes to the map are
     * reflected in the collection, and vice-versa.  If the map is
     * modified while an iteration over the collection is in progress
     * (except through the iterator's own <tt>remove</tt> operation),
     * the results of the iteration are undefined.  The collection
     * supports element removal, which removes the corresponding
     * mapping from the map, via the <tt>Iterator.remove</tt>,
     * <tt>Collection.remove</tt>, <tt>removeAll</tt>,
     * <tt>retainAll</tt> and <tt>clear</tt> operations.  It does not
     * support the <tt>add</tt> or <tt>addAll</tt> operations.
     */
    public Collection<V> values() {
        Collection<V> vs = values;
        return (vs != null ? vs : (values = new Values()));
    }

    private final class Values extends AbstractCollection<V> {

        public Iterator<V> iterator() {
            return newValueIterator();
        }

        public int size() {
            return size;
        }

        public void clear() {
            HashMap.this.clear();
        }
    }

    /**
     * Returns a {@link Set} view of the mappings contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  If the map is modified
     * while an iteration over the set is in progress (except through
     * the iterator's own <tt>remove</tt> operation, or through the
     * <tt>setValue</tt> operation on a map entry returned by the
     * iterator) the results of the iteration are undefined.  The set
     * supports element removal, which removes the corresponding
     * mapping from the map, via the <tt>Iterator.remove</tt>,
     * <tt>Set.remove</tt>, <tt>removeAll</tt>, <tt>retainAll</tt> and
     * <tt>clear</tt> operations.  It does not support the
     * <tt>add</tt> or <tt>addAll</tt> operations.
     *
     * @return a set view of the mappings contained in this map
     */
    public Set<Map.Entry<K, V>> entrySet() {
        return entrySet0();
    }

    private Set<Map.Entry<K, V>> entrySet0() {
        Set<Map.Entry<K, V>> es = entrySet;
        return es != null ? es : (entrySet = new EntrySet());
    }

    private final class EntrySet extends AbstractSet<Map.Entry<K, V>> {
        public Iterator<Map.Entry<K, V>> iterator() {
            return newEntryIterator();
        }

        public int size() {
            return size;
        }

        public void clear() {
            HashMap.this.clear();
        }
    }

    // These methods are used when serializing HashSets
    int capacity() { return table.length; }

    float loadFactor() { return loadFactor; }
}

