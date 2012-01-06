package com.nexr.cache.develop;

import java.util.*;
import java.nio.ByteBuffer;

import com.nexr.cache.MemoryPool;

public abstract class BufferPool implements MemoryPool {

    public static final int DEFAULT_NORMALIZE_FACTOR = 10;  // 1K

    final int normalize;

    int allocated;
    int released;
    int failed;
    int merged;

    // length, offset
    private NavigableSet<int[]> sorted = new TreeSet<int[]>(
            new Comparator<int[]>() {
                public int compare(int[] o1, int[] o2) {
                    if (o1[0] != o2[0]) { return o1[0] - o2[0]; }
                    return o1[1] - o2[1];
                }
            }
    ) {
        public String toString() {
            Iterator<int[]> i = iterator();
            if (!i.hasNext()) {
                return "[]";
            }

            StringBuilder sb = new StringBuilder();
            sb.append('[');
            for (; ;) {
                int[] e = i.next();
                sb.append(e[0]).append(':').append(e[1]);
                if (!i.hasNext()) {
                    return sb.append(']').toString();
                }
                sb.append(", ");
            }
        }
    };
    private Map<Integer, int[]> indexed = new HashMap<Integer, int[]>() {
        public String toString() {
            Iterator<Map.Entry<Integer, int[]>> i = entrySet().iterator();
            if (!i.hasNext()) {
                return "{}";
            }

            StringBuilder sb = new StringBuilder();
            sb.append('{');
            for (; ;) {
                Map.Entry<Integer, int[]> e = i.next();
                Integer key = e.getKey();
                int[] value = e.getValue();
                sb.append(key).append('=').append(value[0]).append(':').append(value[1]);
                if (!i.hasNext()) {
                    return sb.append('}').toString();
                }
                sb.append(", ");
            }
        }
    };

    public BufferPool() {
        this.normalize = DEFAULT_NORMALIZE_FACTOR;
    }

    public BufferPool(int length, int normalize) {
        this.normalize = normalize;
    }

    protected abstract void initialize(int length);

    public int[] allocate(int length) {
        return reserve(length);
    }

    public int[] poll(int length) {
        if (length > capacity()) {
            throw new IllegalArgumentException("requested " + length + ", for cache limit " + capacity());
        }
        int[] result;
        while ((result = reserve(length)) == null) {
            Thread.yield();
        }
        return result;
    }

    public int[] poll(int length, int count) {
        int[] result;
        while ((result = reserve(length)) == null && count-- > 0) {
            Thread.yield();
        }
        return result;
    }

    private int[] reserve(int length) {
        int normalized = normalize(length);
        int[] result = forLength(normalized);
        if (result == null) {
            failed++;
        } else {
            allocated++;
        }
        return result;
    }

    public void release(int[] cached) {
        if (cached != null) {
            released++;
//            System.out.println("[BufferPool/release] " + Thread.currentThread() + " --> " + cached);
            int[] index = forOffset(cached[1]);               // prev index
            if (index != null) {
                merged++;
                put(index(index[0] + cached[0], index[1]));   // merge
            } else {
                put(cached);
            }
        }
    }

    public String toString(int[] index) {
        return null;
    }

    public void release(int offset, int length) {
        released++;
        int[] index = forOffset(offset);               // prev index
        if (index != null) {
            merged++;
            put(index(index[0] + length, index[1]));   // merge
        } else {
            put(index(length, offset));
        }
    }

    public synchronized long used() {
        return capacity() - free();
    }

    public synchronized long free() {
        int total = 0;
        for (int[] index : indexed.values()) {
            total += index[0];
        }
        return total;
    }

    public synchronized int biggest() {
        return sorted.isEmpty() ? 0 : sorted.first()[0];
    }

    public synchronized void clear() {
        sorted.clear();
        indexed.clear();
        put(index((int) capacity(), 0));
    }

    public ByteBuffer expand(int delta) {
        throw new UnsupportedOperationException("expand");
    }

    public ByteBuffer expand(ByteBuffer nbuffer) {
        throw new UnsupportedOperationException("expand");
    }

    protected abstract ByteBuffer failed(int length);

    protected abstract ByteBuffer slice(int length, int start);

    protected synchronized int[] forLength(int length) {
        int[] index = sorted.ceiling(index(length, 0));
        if (index == null) {
            return null;
        }
        sorted.remove(index);
        if (index[0] == length) {
            indexed.remove(index[0] + index[1]);
            return index;
        }
        int[] result = index(length, index[1]);
        index[0] -= length;
        index[1] += length;
        sorted.add(index);
        return result;
    }

    protected synchronized int[] forOffset(int offset) {
        int[] index = indexed.get(offset);
        if (index != null) {
            remove(index);
        }
        return index;
    }

    protected synchronized void put(int[] index) {
        sorted.add(index);
        indexed.put(index[0] + index[1], index);
    }

    protected synchronized void remove(int[] index) {
        sorted.remove(index);
        indexed.remove(index[0] + index[1]);
    }

    protected int[] index(int length, int offset) {
        return new int[] {length, offset};
    }

    protected int normalize(int length) {
        int remain = length % (1 << normalize);
        if (remain == 0) {
            return length;
        }
        return ((length >> normalize) + 1) << normalize;
    }

    public String toString() {
        return sorted.toString() + "\n" + indexed.toString() + "\n" + allocated + "/" + released + ", " + failed + "/" + merged + ", " + sorted.size() + "/" + indexed.size() + "/" + (free() >> 10) +"KB";
    }
}
