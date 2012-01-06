package com.nexr.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.nexr.cache.Region.*;

public abstract class MemcachedRegionPool<T> extends AbstractRegionPool<T> {

    private static final int SLAB_RESERVE = 4;
    private static final int INDEX_RESERVE = 10;

    private static final int SLAB_COUNT_MAX = 1 << 16;      // short

    int slabsize;

    int[] index;
    RegionRack<T>[] racks;

    public void initialize(int start, float increment, int slabsize) {
        this.slabsize = slabsize;
        this.index = initializeIndex(start, increment);
        this.racks = initializeRacks();
    }

    protected int[] initializeIndex(int start, float increment) {
        float next = 1 + increment;
        int value1 = (int) (Math.log10(start) / Math.log10(next));
        int value2 = (int) (Math.log10(Integer.MAX_VALUE) / Math.log10(next));
        int[] index = new int[value2 - value1];

        index[0] = start;
        for (int i = 1; i < index.length; i++) {
            index[i] = (int) (index[i - 1] * next);
        }
        return index;
    }

    @SuppressWarnings("unchecked")
    private RegionRack<T>[] initializeRacks() {
        RegionRack<T>[] racks = new RegionRack[index.length];
        for (int i = 0; i < racks.length; i++) {
            racks[i] = newRack(slabsize);
        }
        return racks;
    }

    protected abstract RegionRack<T> newRack(int slabsize);

    public int[] allocate(int length) {
        int rindex = rackIndex(length);
        if (rindex > index.length) {
            throw new IllegalArgumentException("too large : " + rindex);
        }
        int rlength = rackLength(rindex);
        int[] index = racks[rindex].search(rlength);
        if (index != null) {
            index[LENGTH_INDEX] = length;           // update
        }
        return index;
    }

    public void release(int[] index) {
        int length = index[LENGTH_INDEX];
        int rindex = rackIndex(length);
        racks[rindex].release(index);
    }

    public T regionFor(int[] index, boolean slice) {
        int rindex = rackIndex(Region.toLength(index));
        return racks[rindex].regionFor(index, slice);
    }

    public T regionFor(int[] index, int coffset, int clength) {
        int rindex = rackIndex(Region.toLength(index));
        return racks[rindex].regionFor(index, coffset, clength);
    }

    protected final int rackIndex(int length) {
        int rindex = Arrays.binarySearch(index, length);
        if (rindex < 0) {
            return -rindex - 1;
        }
        return rindex;
    }

    protected final int rackLength(int rindex) {
        return index[rindex];
    }

    protected final RegionRack<T> rackFor(int rindex) {
        return racks[rindex];
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < racks.length; i++) {
            Queue<int[]> pooled = racks[i].pooled;
            builder.append(i).append(":pooled[").append(pooled.size()).append("]=");
            int j = 1;
            int psize = pooled.size();
            for (int[] index : pooled) {
                builder.append(index[0]).append(':').append(index[1]);
                if (j < psize) {
                    builder.append(',');
                }
                j++;
            }
            builder.append('\n');

            List<int[]> reserved = racks[i].reserved;
            builder.append(i).append(":reserved[").append(reserved.size()).append("]=");
            int rsize = reserved.size();
            for (int k = 0; k < rsize; k++) {
                int[] index = reserved.get(k);
                builder.append(index[0]).append(':').append(index[1]);
                if (k + 1 < rsize) {
                    builder.append(',');
                }
            }
            builder.append('\n');
        }
        return builder.toString();
    }

    protected static abstract class RegionRack<T> {

        private final Queue<int[]> pooled;  // rindex : List<index(slab:offset:length)>
        private final List<int[]> reserved; // rindex : List<slab-index(slab:offset:length)>

        // rack : slab
        private final int slabsize;
        private final List<T> regions;

        public RegionRack(int slabsize) {
            this.slabsize = slabsize;
            this.pooled = new LinkedBlockingQueue<int[]>();
            this.reserved = new ArrayList<int[]>();
            this.regions = new ArrayList<T>();
        }

        public void release(int[] index) {
            pooled.offer(index);
        }

        public T regionFor(int[] index, boolean slice) {
            T region = regionFor(index);
            return slice ? slice(region, index, 0, -1) : region;
        }

        public T regionFor(int[] index, int coffset, int clength) {
            T region = regionFor(index);
            return slice(region, index, coffset, clength);
        }

        protected T slice(T buffer, int[] index, int coffset, int clength) {
            return buffer;
        }

        protected T regionFor(int[] index) {
            return regionFor(Region.toSlabs(index));
        }

        protected final T regionFor(int sindex) {
            synchronized (regions) {
                return sindex < regions.size() ? regions.get(sindex) : null;
            }
        }

        protected final T recoverFor(int rlength, int sindex) {
            synchronized (regions) {
                int current = regions.size();
                if (sindex < current) {
                    return regions.get(sindex);
                }
                for (int index = sindex; index <= current; index++) {
                    Region<T> region = region(index, rlength, slabsize);
                    regions.add(index, region.region);
                    reserved.add(region.index);
                }
                return regions.get(sindex);
            }
        }

        protected int[] updateIndex(int sindex, int poffset) {
            int[] master = reserved.get(sindex);
            master[Region.OFFSET_INDEX] = poffset;
            master[Region.LENGTH_INDEX] -= poffset;
            return master;
        }

        public int[] search(int rlength) {
            int[] index = searchPool();
            if (index != null) {
                return index;
            }
            return newIndex(rlength);
        }

        private int[] searchPool() {
            return pooled.poll();
        }

        private synchronized int[] newIndex(int rlength) {
            int[] index = fromReserved(rlength);
            if (index != null) {
                return index;
            }
            return fromSlab(rlength);
        }

        private int[] fromReserved(int rlength) {
            int length = reserved.size();
            if (length == 0) {
                return null;
            }
            int sindex = length - 1;
            int[] master = reserved.get(sindex);
            if (master[LENGTH_INDEX] >= rlength) {
                int[] index = Region.index(sindex, master[OFFSET_INDEX], rlength);
                master[OFFSET_INDEX] += rlength;
                master[LENGTH_INDEX] -= rlength;
                return index;
            }
            return null;
        }

        private int[] fromSlab(int rlength) {
            int sindex = regions.size();
            if (sindex >= SLAB_COUNT_MAX) {
                return null;
            }
            Region<T> region = region(sindex, rlength, slabsize);
            if (region == null) {
                return null;
            }
            region.index[OFFSET_INDEX] += rlength;
            region.index[LENGTH_INDEX] -= rlength;

            regions.add(region.region);
            reserved.add(region.index);

            return Region.index(sindex, 0, rlength);
        }

        protected abstract Region<T> region(int sindex, int rlength, int slabsize);
    }
}
