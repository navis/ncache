package com.nexr.cache;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static com.nexr.cache.Constants.*;
import com.nexr.cache.util.Generations;
import com.nexr.cache.util.LRUSet;

@SuppressWarnings("unchecked")
public class KVIndexCache extends LRUSet<int[][]> {

    private static final long SYNC_POLLING = 100;

    final long[] generations;

    AtomicInteger recovery;

    RecordManager records;
    PersistentPool storage;

    public KVIndexCache(RecordManager records) {
        this(records, null, 2);
    }

    public KVIndexCache(RecordManager records, PersistentPool storage, int maxReplication) {
        super(records.capacity());
        this.records = records;
        this.storage = storage;
        this.recovery = new AtomicInteger();
        this.generations = new long[maxReplication];
    }

    synchronized long next(int pindex) {
        return ++generations[pindex];
    }

    synchronized void markGeneration(int pindex, long generation) {
        generations[pindex] = generations[pindex] > generation ? generations[pindex] : generation;
        System.out.println("[KVIndexCache/markGeneration] " + Arrays.toString(generations));
    }

    public void awaitRecovery(long timeout) {
        try {
            for (; timeout > 0 && onRecovery(); timeout -= SYNC_POLLING) {
                Thread.sleep(SYNC_POLLING);
            }
        } catch (InterruptedException e) {
            // ignore
        }
        if (onRecovery()) {
            throw new IllegalStateException("unsynchronized partition");
        }
    }

    private boolean onRecovery() {
        return recovery.get() != 0;
    }

    @Override
    protected int[][] accessed(Entry<int[][]> target) {
        return records.isRemoved(target.value()[MEMORY]) ? null : super.accessed(target);
    }

    @Override
    protected void marking(Entry<int[][]> target) {
        records.cropData(target.value());
        super.marking(target);
    }

    public Entry<int[][]> store(int pindex, int[][] eindex) {
        markGeneration(pindex, Generations.COUNTER(eindex[INFO]));
        Entry<int[][]> prev = put(eindex, eindex[INFO][INFO_HASH]);
        return prev == null ? null : releaseAll(prev);
    }

    public Entry<int[][]> store(int pindex, int[][] eindex, ConflictResolver<int[][]> resolver) {
        markGeneration(pindex, Generations.COUNTER(eindex[INFO]));
        Entry<int[][]> prev = put(eindex, eindex[INFO][INFO_HASH], resolver);
        return prev == null ? null : releaseAll(prev);
    }

    public Entry<int[][]> remove(int pindex, byte[] key, int[] information, boolean markOnly) {
        markGeneration(pindex, Generations.COUNTER(information));
        if (markOnly) {
            Entry<int[][]> removed = removeMark(key, information);
            return removed == null ? null : releaseStorage(removed);
        }
        Entry<int[][]> removed = remove(key, information[INFO_HASH]);
        return removed == null ? null : releaseAll(removed);
    }

    private synchronized Entry<int[][]> removeMark(byte[] key, int[] information) {
        System.out.println("[KVIndexCache/removeMark] " + new String(key));
        Entry<int[][]> marked = removeMark(key, information[INFO_HASH]);
        if (marked != null) {
            int[][] index = marked.value();
            index[INFO][INFO_GSLOW] = information[INFO_GSLOW];
            index[INFO][INFO_GSHIGH] = information[INFO_GSHIGH];        }
        return marked;
    }

    public Entry<int[][]> remove(int pindex, Object key, int[] information, ConflictResolver<int[]> resolver, boolean markOnly) {
        markGeneration(pindex, Generations.COUNTER(information));
        if (markOnly) {
            Entry<int[][]> removed = removeMark(key, information, resolver);
            return removed == null ? null : releaseStorage(removed);
        }
        Entry<int[][]> removed = remove(key, information, resolver);
        return removed == null ? null : releaseAll(removed);
    }

    private synchronized Entry<int[][]> remove(Object key, int[] information, ConflictResolver<int[]> resolver) {
        return remove(information, key, information[INFO_HASH], resolver);
    }

    private synchronized Entry<int[][]> removeMark(Object key, int[] information, ConflictResolver<int[]> resolver) {
        Entry<int[][]> marked = removeMark(information, key, information[INFO_HASH], resolver);
        if (marked != null) {
            int[][] index = marked.value();
            index[INFO][INFO_GSLOW] = information[INFO_GSLOW];
            index[INFO][INFO_GSHIGH] = information[INFO_GSHIGH];
        }
        return marked;
    }

    public Entry<int[][]> expired(int[][] key, int hash) {
        Entry<int[][]> removed = remove(key, hash);
        return removed == null ? null : releaseMemory(removed);
    }

    public int[] retrieve(String key) {
        byte[] keybytes = key.getBytes();
        return retrieve(keybytes, Utils.hash(keybytes));
    }

    public int[] retrieve(Object key, int hash) {
        int[][] retrieved = get(key, hash);
        return retrieved == null || tryExpire(retrieved, hash) ? null : retrieved[MEMORY];
    }

    protected final int toLength(int[][] index) {
        return Region.toLength(index[MEMORY]);
    }

    protected final String toString(int[][] index) {
        return Region.toString(index[MEMORY]);
    }

    private Entry<int[][]> releaseAll(Entry<int[][]> entry) {
        int[][] index = entry.value();
        records.release(index[MEMORY]);
        if (index.length > 2 && index[PERSIST] != null) {
            storage.remove(index[PERSIST]);
        }
        return entry;
    }

    private Entry<int[][]> releaseMemory(Entry<int[][]> entry) {
        int[][] index = entry.value();
        records.release(index[MEMORY]);
        return entry;
    }

    private Entry<int[][]> releaseStorage(Entry<int[][]> entry) {
        int[][] index = entry.value();
        if (index.length > 2 && index[PERSIST] != null) {
            storage.remove(index[PERSIST]);
        }
        return entry;
    }

    @Override
    protected final int hashFor(int[][] index) {
        return index[INFO][INFO_HASH];
    }

    @Override
    protected final boolean lookup(Object key, int[][] stored) {
        return key instanceof byte[] ? equals((byte[]) key, stored) : equals((int[][]) key, stored);
    }

    @Override
    protected final boolean equals(int[][] index1, int[][] index2) {
        return records.equals(index1[MEMORY], index2[MEMORY]);
    }

    protected final boolean equals(byte[] key, int[][] stored) {
        return records.equals(key, stored[MEMORY]);
    }

    public void dump() {
        for (int[][] index : values()) {
            System.out.println(records.keyString(index[MEMORY]));
        }
    }

    public void flush() {
        for (int[][] index : clearAll()) {
            records.release(index[MEMORY]);
        }
    }

    public void expire(int maximum) {
        int current = Utils.currentSeconds();
        for (int i = 0; i < maximum; i++) {
            Iterator<int[][]> iterator = cursor();
            while (iterator.hasNext()) {
                tryExpire(current, iterator.next());
            }
        }
    }

    private boolean tryExpire(int[][] eindex, int hash) {
        return tryExpire(Utils.currentSeconds(), eindex, hash);
    }

    private boolean tryExpire(int current, int[][] eindex) {
        return tryExpire(current, eindex, eindex[INFO][INFO_HASH]);
    }

    private boolean tryExpire(int current, int[][] eindex, int hash) {
        int expire = records.expireTime(eindex[MEMORY]);
        if (expire > 0 && expire <= current) {
            expired(eindex, hash);     // no generation update
            return true;
        }
        return false;
    }

    public void search(int sindex, long counter, DataCollector collector) {
        for (int[][] eindex : cvalues()) {
            int[] information = eindex[INFO];
            System.out.println("[KVIndexCache/search] try " + Generations.TO_STRING(information) + ", " + Region.toString(eindex[MEMORY]) + "=" + records.keyValueString(eindex[MEMORY]));
            if (Generations.SERVER(information) != sindex) {
                continue;
            }
            if (Generations.COUNTER(information) > counter) {
                System.out.println("-- [KVIndexCache/search] pushed !! " + Region.toString(eindex[MEMORY]));
                collector.push(eindex[MEMORY]);
            }
        }
        collector.flush();
    }
    
    public long recovering(int pindex, long remote) {
        if (remote > 0 && generations[pindex] < remote) {
            recovery.incrementAndGet();
            return generations[pindex];
        }
        return -1;
    }

    public boolean recovered() {
        return recovery.decrementAndGet() == 0;
    }
}
