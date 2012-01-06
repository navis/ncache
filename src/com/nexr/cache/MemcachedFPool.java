package com.nexr.cache;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import static java.nio.file.StandardOpenOption.*;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MemcachedFPool extends MemcachedRegionPool<FileChannel> implements PersistentPool {

    File directory;
    MemoryPool pool;
    IndexFiles indexFile;

    public MemcachedFPool(MemoryPool pool, File directory) {
        this.pool = pool;
        this.directory = directory;
        this.indexFile = new IndexFiles(directory, pool);
        if (!directory.exists() && !directory.mkdirs()) {
            throw new IllegalStateException("faild to create directory " + directory);
        }
    }

    public void initialize(Initializer init) throws IOException {
        List<int[]> indexes = indexFile.intialize(init.compact());
        for (int[] pindex : indexes) {
            System.out.println("[MemcachedFPool/initialize] " + Region.toString(pindex));
        }
        Collections.sort(indexes, new Comparator<int[]>() {
            public int compare(int[] o1, int[] o2) {
                int compared = o1[Region.LENGTH_INDEX] - o2[Region.LENGTH_INDEX];
                if (compared == 0) {
                    compared = o1[Region.SLAB_INDEX] - o2[Region.SLAB_INDEX];
                    if (compared == 0) {
                        return o1[Region.OFFSET_INDEX] - o2[Region.OFFSET_INDEX];
                    }
                }
                return compared;
            }
        });

        int prindex = -1;
        int psindex = -1;
        int poffset = -1;
        int rlength = -1;

        FileChannel region = null;
        RegionRack<FileChannel> rack = null;

        for (int[] pindex : indexes) {
            int sindex = Region.toSlabs(pindex);
            int offset = Region.toOffset(pindex);
            int length = Region.toLength(pindex);
            int rindex = rackIndex(length);
            if (rack == null || prindex != rindex) {
                rack = rackFor(rindex);
                rlength = rackLength(rindex);
                region = rack.recoverFor(rlength, sindex);    // switched
                poffset = -1;
            } else if (psindex != sindex) {
                if (psindex >= 0) {
                    rack.updateIndex(psindex, poffset);
                }
                region = rack.recoverFor(rlength, sindex);    // switched
                poffset = -1;
            }
            int[] mindex = pool.allocate(length);
            ByteBuffer buffer = pool.regionFor(mindex, true);
            region.read(buffer, offset);

            init.keyvalue(mindex, pindex);

            if (poffset > 0) {
                for (; poffset < offset; poffset += rlength) {
                    rack.release(Region.index(psindex, poffset, rlength));
                }
            }
            prindex = rindex;
            psindex = sindex;
            poffset = offset + rlength;
        }
    }

    public void synchronize(FileChannel channel) throws IOException {
        channel.force(false);
    }

    public int[] store(int[] mindex) {
        ByteBuffer region = pool.regionFor(mindex, true);
        int[] pindex = allocate(Region.toLength(mindex));
        try {
            FileChannel channel = regionFor(pindex, false);
            channel.write(region, Region.toOffset(pindex));
            indexFile.put(pindex);
        } catch (Exception e) {
            release(pindex);
            return null;
        }
        return pindex;
    }

    public int[] remove(int[] pindex) {
        try {
            return indexFile.remove(pindex);
        } catch (Exception e) {
            // ignore
        } finally {
            release(pindex);
        }
        return null;
    }

    protected RegionRack<FileChannel> newRack(int slabSize) {
        return new StorageRack(directory, slabSize);
    }

    private static class StorageRack extends RegionRack<FileChannel> {

        File directory;

        public StorageRack(File directory, int slabsize) {
            super(slabsize);
            this.directory = directory;
        }

        protected Region<FileChannel> region(int sindex, int rlength, int slabsize) {
            try {
                File file = new File(directory, "data." + rlength + "." + sindex);
                FileChannel channel = FileChannel.open(file.toPath(), CREATE, READ, WRITE);
                return new Region<FileChannel>(channel, sindex, slabsize);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}

