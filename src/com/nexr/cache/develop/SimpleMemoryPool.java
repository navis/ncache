package com.nexr.cache.develop;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.nexr.cache.util.InherentPriorityQueue;
import com.nexr.cache.util.LinkedSet;
import com.nexr.cache.AbstractMemoryPool;
import com.nexr.cache.MemoryPool;
import com.nexr.cache.Region;

public class SimpleMemoryPool extends AbstractMemoryPool implements MemoryPool, Runnable {

    private static final int SLAB_ALLOCATION = 32 << 20;
    private static final int SLAB_MAXIMUM = 65536;

    private static final int SLAB_IDX = 0;
    private static final int OFFSET_IDX = 1;
    private static final int LENGTH_IDX = 2;

    MemorySlabs heap = new MemorySlabs(SLAB_MAXIMUM);
//    MemorySlabsList heap = new MemorySlabsList();
    List<MemorySlab> islabs = new ArrayList<MemorySlab>();

    LinkedBlockingQueue<MemorySlab> fulled = new LinkedBlockingQueue<MemorySlab>();

    public SimpleMemoryPool() {
        newSlab(SLAB_ALLOCATION);
    }

    public void start() {
        Thread thread = new Thread(this);
        thread.setName("COMPACTION");
        thread.setDaemon(true);
        thread.start();
    }

    protected ByteBuffer reserveBuffer(int length) {
        System.out.println("[SimpleMemoryPool/reserveBuffer] " + (length >> 20) + "M");
        return ByteBuffer.allocateDirect(length);
    }

    public long capacity() {
        return SLAB_ALLOCATION * SLAB_MAXIMUM;
    }

    public int[] allocate(int length) {
        if (length == 0) {
            return new int[] {0, 0, 0};
        }
        int count = 0;
        MemorySlab slab = heap.top();
        do {
            int[] index = reserve(slab, length);
            if (index != null) {
                return index;
            }
            Thread.yield();
        } while ((count++ < 3 && (slab = heap.top()) != null) || (slab = nextSlab(length)) != null);

        return null;
    }

    public void release(int[] index) {
        MemorySlab slab = islabs.get(index[SLAB_IDX]);
        slab.release(index);
//            slab.compact();
//            heap.upHeap(slab);
//            System.out.println("[SimpleMemoryPool/release] " + heap);
//            fulled.offer(slab);
    }

    private int[] reserve(MemorySlab slab, int length) {
        int[] index = slab.reserve(length);
        if (index != null) {
            heap.downHeap(slab);
        }
        return index;
    }

    public int toOffset(int[] index) {
        return index[OFFSET_IDX];
    }

    public int toLength(int[] index) {
        return index[LENGTH_IDX];
    }

    public ByteBuffer regionFor(int[] index, boolean slice) {
        MemorySlab slab = islabs.get(index[SLAB_IDX]);
        return slab.bufferFor(index, slice);
    }

    public ByteBuffer regionFor(int[] index, int offset, int length) {
        MemorySlab slab = islabs.get(index[SLAB_IDX]);
        return slab.bufferFor(index, offset);
    }

    public String toString(int[] index) {
        return index[0] + ":" + index[1] + ":" + index[2];
    }

    private MemorySlab nextSlab(int length) {
        return newSlab(Math.max(length, SLAB_ALLOCATION));
    }

    private synchronized MemorySlab newSlab(int length) {
        if (islabs.size() == SLAB_MAXIMUM) {
            return null;
        }
//        System.out.println("[SimpleMemoryPool/newSlab] " + sindex.size());
//        System.out.println(" -- [SimpleMemoryPool/newSlab] " + heap);
        ByteBuffer buffer = reserveBuffer(length);
        MemorySlab allocated = new MemorySlab(islabs.size(), buffer);
        islabs.add(allocated);
        heap.put(allocated);
//        System.out.println(" -- [SimpleMemoryPool/newSlab] " + heap);
        return allocated;
    }

    public void run() {
        while (true) {
            try {
                SimpleMemoryPool.MemorySlab slab = fulled.take();
                slab.compact();
                heap.upHeap(slab);
            } catch (Exception e) {
                e.printStackTrace();
                // ignore
            }
        }
    }

    public static class MemorySlab extends LinkedSet<int[]> implements Comparable<MemorySlab> {

        private static final int FREE = 0;
        private static final int COMPACTING = 1;

        int state;

        int hindex;
        int occupied;

        final int[] master;
        final ByteBuffer buffer;

        public MemorySlab(int slabID, ByteBuffer buffer) {
            super(false);
            this.buffer = buffer;
            this.master = new int[]{slabID, 0, buffer.capacity()};
        }

        public int[] reserve(int length) {
            int[] index = reserveFor(length);
            if (index != null) {
                return index;
            }
            if (doCompact(length)) {
                compact();
            }
            return reserveFor(length);
        }

        private synchronized int[] reserveFor(int length) {
            if (state != FREE || master[LENGTH_IDX] < length) {
                return null;
            }
            int[] slice = new int[]{master[SLAB_IDX], master[OFFSET_IDX], length};
            master[OFFSET_IDX] += length;
            master[LENGTH_IDX] -= length;
            occupied += length;
            put(slice);

            return slice;
        }

        private boolean doCompact(int length) {
            if (master[LENGTH_IDX] < length) {
                return false;
            }
            if (changeState(FREE, COMPACTING)) {
                return true;
            }
            awaitState(FREE);
            return false;
        }

        private synchronized void setState(int state) {
            this.state = state;
        }

        private synchronized boolean awaitState(int expact) {
            try {
                while (state != expact) {
                    wait();
                }
            } catch (InterruptedException e) {
                // ignore
            }
            return state == expact;
        }

        private synchronized boolean changeState(int free, int fulled) {
            if (state != free) {
                return false;
            }
            state = fulled;
            return true;
        }

        public void release(int[] index) {
            releaseFor(index);
            if (doCompact(index[LENGTH_IDX])) {
                compact();
            }
        }

        private synchronized void releaseFor(int[] index) {
            awaitState(FREE);
            if (remove(index) == null) {
                System.out.println("-- [SimpleMemoryPool$MemorySlab/release] failed!! " + toString(index));
                System.out.println("[SimpleMemoryPool$MemorySlab/release] " + dump());
                System.exit(1);
            }
            occupied -= index[LENGTH_IDX];
        }

        @Override
        protected int hashFor(int[] index) {
            return index[OFFSET_IDX];
        }

        @Override
        protected boolean lookup(Object key, int[] stored) {
            return equals((int[]) key, stored);
        }

        @Override
        protected boolean equals(int[] index1, int[] index2) {
            return index1[OFFSET_IDX] == index2[OFFSET_IDX];
        }

        public int compareTo(MemorySlab other) {
            int compared = master[LENGTH_IDX] - other.master[LENGTH_IDX];
            if (compared == 0) {
                return master[SLAB_IDX] - other.master[SLAB_IDX];
            }
            return compared;
        }

        public ByteBuffer bufferFor(int[] index, boolean slice) {
            return slice ? bufferFor(index, 0) : buffer;
        }

        private ByteBuffer bufferFor(int[] index, int start) {
            int offset = Region.toOffset(index);
            int length = Region.toLength(index);
            synchronized (buffer) {
                buffer.limit(offset + length);
                buffer.position(offset + start);
                return buffer.slice();
            }
        }

        public void compact() {
//            System.out.println("-- [SimpleMemoryPool$MemorySlab/compact] start " + this);
            buffer.position(0);
            buffer.limit(buffer.capacity());

            ByteBuffer duplicate = buffer.duplicate();
            for (int[] index : this) {
                int position = buffer.position();
                duplicate.limit(index[OFFSET_IDX] + index[LENGTH_IDX]);
                duplicate.position(index[OFFSET_IDX]);
                index[OFFSET_IDX] = position;
                buffer.put(duplicate);
            }
            master[OFFSET_IDX] = buffer.position();
            master[LENGTH_IDX] = buffer.capacity() - master[OFFSET_IDX];

            rehashAll();
            setState(FREE);
//            System.out.println("[SimpleMemoryPool$MemorySlab/compact] completed " + this);
        }

        private String toString(int[] index) {
            return index[0] + ":" + index[1] + ":" + index[2];
        }

        public String toString() {
            return toString(master) + ":" + state;
        }

        public String dump() {
            StringBuilder builder = new StringBuilder();
            for (int[] value : this) {
                builder.append(value[0]).append(':');
                builder.append(value[1]).append(':');
                builder.append(value[2]);
                builder.append(' ');
            }
            return builder.toString();
        }
    }

    private static class MemorySlabsList extends ArrayList<MemorySlab> {

        MemorySlab[] snapshot = new MemorySlab[0];

        public synchronized MemorySlab[] snapshot() {
            return snapshot;
        }

        public synchronized MemorySlab[] snapshot(MemorySlab fulled) {
            return snapshot;
        }
    }

    private static class MemorySlabs extends InherentPriorityQueue<MemorySlab> {

        public MemorySlabs(int maxSize) {
            super(maxSize, false);
        }

        @Override
        protected void set(int index, MemorySlab element) {
            super.set(index, element);
            element.hindex = index;
        }

        public synchronized void downHeap(MemorySlab slab) {
            downHeap(slab.hindex);
        }

        public synchronized void upHeap(MemorySlab slab) {
            upHeap(slab.hindex);
        }
    }
}
