package com.nexr.cache;

import java.io.File;
import java.io.IOException;

import com.nexr.cache.util.Property;

public class ServerNameSpace extends NameSpace {

    private int[] pindexes;       // pindex --> index of sindex[] of self

    private ServerConnectionManager cluster;
    private PersistentPool storage;
    private PartitionedCache cache;

    public ServerNameSpace(byte index, String name, int limitKB, int replica, int partition, boolean persist) {
        super(index, name, limitKB, replica, partition, persist);
    }

    public void initialize(ServerResource resource) throws IOException {
        this.cluster = resource.cluster();
        this.storage = initializeStorage(resource);
        this.cache = initCache(resource);
    }

    @Override
    public PersistentPool storage() {
        return storage;
    }

    private PersistentPool initializeStorage(ServerResource resource) throws IOException {
        Property property = resource.property();
        String directory = property.get(Property.STORAGE_DIRECTORY);
        if (persist && directory != null) {
            int start = property.getInt(Property.PPOOL_START);
            float increment = property.getFloat(Property.PPOOL_INCREASE);
            int slabsize = property.getInt(Property.PPOOL_SLABSIZE);
            MemcachedFPool pool = new MemcachedFPool(resource.memory(), new File(directory, name));
            pool.initialize(start, increment, slabsize);
            return pool;
        }
        return null;
    }

    private PartitionedCache initCache(ServerResource resource) throws IOException {
        PartitionedCache cache = new PartitionedCache(resource, this);
        cache.intiailize();
        return cache;
    }

    @Override
    public int partitionIndexFor(int partition, int sindex) {
        return sindex == cluster.index() ? pindexes[partition] : super.partitionIndexFor(partition, sindex);
    }

    @Override
    public int partitionIndexForSelf(int partition) {
        return pindexes[partition];
    }

    @Override
    public synchronized void syncPartition(int[][] update) {
        super.syncPartition(update);
        pindexes = indexesFor(update, cluster.index());
        cache.partitioned(partitions());
    }

    public synchronized void stateChanged(int sindex, boolean activated) {
        int[][] partitions = partitions();
        if (partitions != null) {
            System.out.println("-- [ServerNameSpace/stateChanged] " + sindex + ", " + activated);
            if (activated) {
                cache.activated(sindex, partitions);
            } else {
                cache.passivated(sindex, partitions);
            }
        }
    }

    @Override
    public void destroy() {
        cache.flushAll();
    }

    public PartitionedCache cache() {
        return cache;
    }

    public int[][] usage() {
        return cache.usages();
    }

    public long[][] recovery() {
        return new long[partitionNum()][replicaNum()];
    }
}
