package com.nexr.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;

import com.nexr.cache.message.*;
import com.nexr.cache.util.Generations;
import com.nexr.cache.util.LinkedSet;
import com.nexr.cache.util.LinkedSet.Entry;

public class PartitionedCache {

    private static final long SYNC_TIMEOUT = 3000;
    private static final long SYNC_TIMEOUT_FORWARDED = 4000;
    private static final long FORWARD_EXPIRE = 2000;

    final NameSpace namespace;
    final ServerResource resource;
    final ServerConnectionManager cluster;
    final PersistentPool storage;

    final RecordManager records;
    final KVIndexCache[] elements;

    final long[][] snapshots;

    RecoverEntry[] pendings;
    LinkedList<RecoverEntry>[] markers;

    volatile boolean desync;

    public PartitionedCache(ServerResource resource, NameSpace namespace) {
        this.resource = resource;
        this.namespace = namespace;
        this.cluster = resource.cluster();
        this.storage = namespace.storage();
        this.records = initRecordManager(namespace.limitKB());
        this.elements = initPartitionHash(namespace.replicaNum());
        this.snapshots = initSnapshots(namespace.partitionNum());
        this.markers = initRecoveryMarker(namespace.replicaNum());
        this.pendings = new RecoverEntry[namespace.replicaNum()];
    }

    private RecordManager initRecordManager(int limitKB) {
        return new RecordManager(resource.memory(), limitKB);
    }

    private KVIndexCache[] initPartitionHash(int maxReplication) {
        KVIndexCache[] indexes = new KVIndexCache[namespace.partitionNum()];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = new KVIndexCache(records, storage, maxReplication);
        }
        return indexes;
    }

    private long[][] initSnapshots(int partitions) {
        long[][] snapshots = new long[partitions][];
        for (int partition = 0; partition < snapshots.length; partition++) {
            snapshots[partition] = elements[partition].generations;
        }
        return snapshots;
    }

    private SyncResolver[] initResolvers(int partitions) {
        SyncResolver[] resolvers = new SyncResolver[partitions];
        for (int partition = 0; partition < resolvers.length; partition++) {
            resolvers[partition] = new SyncResolver(namespace, partition);
        }
        return resolvers;
    }

    public void intiailize() throws IOException {
        loadStorage();
        resource.execute(new ExpireManager());
    }

    private void loadStorage() throws IOException {
        if (storage != null) {
            storage.initialize(new PersistentPool.Initializer() {
                public boolean compact() { return true; }

                public void keyvalue(int[] mindex, int[] pindex) {
                    replay(mindex, pindex);
                }
            });
        }
    }

    public void sync(SyncPushed request) {
        for (StreamingMessage.Entry entry : request.entries()) {
            int partition = namespace.partitionIDForHash(entry.hash);
            int[] information = Generations.INDEX(entry.genHigh, entry.genLow, entry.hash);
            if (entry.isRemoved()) {
                System.out.println("-- [PartitionedCache/sync] remove " + records.keyString(entry.mindex) + " --> partition = " + partition + ", " + Generations.SERVER(information));
                int pindex = namespace.partitionIndexFor(partition, Generations.SERVER(information));
                elements[partition].remove(pindex, entry.mindex, information, desync);
            } else {
                System.out.println("-- [PartitionedCache/sync] store  " + records.keyString(entry.mindex) + " --> partition = " + partition + ", " + Generations.SERVER(information));
                int[][] eindex = kvindex(information, entry.mindex, entry.isPersist());
                int pindex = namespace.partitionIndexFor(partition, Generations.SERVER(information));
                elements[partition].store(pindex, eindex, information);
            }
        }
    }

    public void store(ClientPushed request) {
        int partition = namespace.partitionIDForHash(request.hash);
        int pindex = namespace.partitionIndexForSelf(partition);

        elements[partition].awaitRecovery(SYNC_TIMEOUT);
        request.reviseData(pindex, elements[partition].next(pindex));
        int[] information = Generations.INDEX(request.generation, request.hash);
        int[][] eindex = kvindex(information, request.index, request.isPersist());

        System.out.println("-- [PartitionedCache/storeM] K:" + records.keyString(request.index) + ", " + Generations.TO_STRING(information));
        System.out.println("-- [PartitionedCache/storeM] P:" + partition + "=" + Arrays.toString(namespace.partitionFor(partition)) + ", pindex=" + pindex);

        Entry entry = elements[partition].store(pindex, eindex);
        if (entry != null) {
            markRecovery(entry, partition);
        }
        forwardStore(partition, request.index);
    }

    public void store(StoreForwarded request) {
        int partition = namespace.partitionIDForHash(request.hash);
        int[] information = Generations.INDEX(request.genHigh, request.genLow, request.hash);
        int[][] eindex = kvindex(information, request.index, request.isPersist());

        int pindex = namespace.partitionIndexFor(partition, Generations.SERVER(information));
        System.out.println("-- [PartitionedCache/storeS] K:" + records.keyString(request.index) + ", " + Generations.TO_STRING(information));
        System.out.println("-- [PartitionedCache/storeS] P:" + partition + "=" + Arrays.toString(namespace.partitionFor(partition)) + ", pindex=" + pindex);

        elements[partition].awaitRecovery(SYNC_TIMEOUT_FORWARDED);
        Entry entry = elements[partition].store(pindex, eindex, resolver);
        if (entry != null) {
            markRecovery(entry, partition, request.index());
        }
        forwardStore(partition, request.index);
    }

    private void forwardStore(int partition, int[] mindex) {
        int pindex = namespace.partitionIndexForSelf(partition);
        for (pindex++; pindex < namespace.replicaNum(); pindex++) {
            if (forwardStoreTo(partition, pindex, mindex)) {
                break;
            }
        }
    }

    private boolean forwardStoreTo(final int partition, final int pindex, final int[] mindex) {
        ServerEntity forwarding = cluster.serverFor(namespace.partitionFor(partition, pindex));
        if (!forwarding.isActive()) {
            return false;
        }
        System.out.println("---- [PartitionedCache/forwardStoreTo] " + forwarding);
        forwarding.notify(new Forward(mindex, namespace.index()) {
            long expire = Utils.afterMillis(FORWARD_EXPIRE);
            public boolean isExpired() {
                if (expire > Utils.current()) { return false; }
                System.out.println("---- [PartitionedCache/forwardStoreTo] expired !!!!!!!!!!!!");
                // todo forward recovery mark
                if (pindex + 1 < namespace.replicaNum()) {
                    forwardStoreTo(partition, pindex + 1, mindex);
                }
                return true;
            }
        });
        return true;
    }

    public void remove(Remove remove) {
        int partition = namespace.partitionIDForHash(remove.hash);
        int[] information = Generations.INDEX(cluster.index(), elements[partition].next(), remove.hash);

        int pindex = namespace.partitionIndexFor(partition, Generations.SERVER(information));
        System.out.println("-- [PartitionedCache/removeM] K:" + new String(remove.key) + ", " + Generations.TO_STRING(information));
        System.out.println("-- [PartitionedCache/removeM] P:" + partition + "=" + Arrays.toString(namespace.partitionFor(partition)) + ", pindex=" + pindex);

        elements[partition].awaitRecovery(SYNC_TIMEOUT);

        boolean markOnly = desync || pindex > 0;
        Entry entry = elements[partition].remove(pindex, remove.key, information, markOnly);
        if (entry != null) {
            markRecovery(entry, partition);
        }
        forwardRemove(partition, information, remove.key);
    }

    public void remove(RemoveForwarded remove) {
        int partition = namespace.partitionIDForHash(remove.hash);

        int pindex = namespace.partitionIndexForSelf(partition);
        System.out.println("-- [PartitionedCache/removeS] K:" + new String(remove.rkey) + ", " + Generations.TO_STRING(remove.rinformation));
        System.out.println("-- [PartitionedCache/removeS] P:" + partition + "=" + Arrays.toString(namespace.partitionFor(partition)) + ", pindex=" + pindex);

        elements[partition].awaitRecovery(SYNC_TIMEOUT_FORWARDED);

        boolean markOnly = desync || namespace.isPrevious(partition, remove.index());
        Entry entry = elements[partition].remove(pindex, remove.rkey, remove.rinformation, markOnly);
        if (entry != null) {
            markRecovery(entry, partition, remove.index());
        }
        forwardRemove(partition, remove.rinformation, remove.rkey);
    }

    private void forwardRemove(int partition, int[] information, byte[] key) {
        int pindex = namespace.partitionIndexForSelf(partition);
        for (pindex++; pindex < namespace.replicaNum(); pindex++) {
            if (forwardRemoveTo(pindex, partition, information, key)) {
                break;
            }
        }
    }

    private boolean forwardRemoveTo(final int partition, final int pindex, final int[] information, final byte[] key) {
        ServerEntity forwarding = cluster.serverFor(namespace.partitionFor(partition, pindex));
        if (!forwarding.isActive()) {
            return false;
        }
        System.out.println("---- [PartitionedCache/forwardRemoveTo] " + forwarding);
        forwarding.notify(new RemoveForward(key, information, namespace.index()) {
            long expire = Utils.afterMillis(FORWARD_EXPIRE);
            public boolean isExpired() {
                if (expire > Utils.current()) { return false; }
                System.out.println("---- [PartitionedCache/forwardRemoveTo] expired !!!!!!!!!!!!");
                // todo forward recovery mark
                if (pindex + 1 < namespace.replicaNum()) {
                    forwardRemoveTo(partition, pindex + 1, information, key);
                }
                return true;
            }
        });
        return true;
    }

    private void markRecovery(Entry entry, int partition) {
        int pindexe = namespace.partitionIndexForSelf(partition);
        markRecovery(entry, partition, pindexe, pindexe, pindexe == 0);
    }

    private void markRecovery(Entry entry, int partition, int sprevious) {
        if (!namespace.isPrevious(partition, sprevious)) {
            int pindexs = namespace.partitionIndexFor(partition, sprevious);
            int pindexe = namespace.partitionIndexForSelf(partition);
            markRecovery(entry, partition, pindexs, pindexe);
        }
    }

    private void markRecovery(Entry entry, int partition, int pindexs, int pindexe, boolean sync) {
        for (int pindex = 0; pindex < pindexe; pindex++) {
            if (pindex > pindexs) {
                markDesync(entry, partition, pindex);
            } else {
                markSync(entry, partition, pindex);
            }
        }
    }

    private void markDesync(Entry entry, int partition, int pindex) {
        RecoverEntry recover = start(entry, pindex);
        if (pendings[pindex] == null) {
            pendings[pindex] = new RecoverEntry(entry);
            desync = true;
        }
    }

    private void markSync(Entry entry, int partition, int pindex) {
        RecoverEntry recover = end(entry, pindex);
        if (recover != null) {
            ServerEntity server = cluster.serverFor(namespace.partitionFor(partition, pindex));
            if (server.isActive() && server.isConnected()) {
                DataCollector collector = new DataCollector(namespace.index(), server);
                for (Entry<int[]> ventry : pendings[pindex]) {
                    collector.push(ventry.value());
                }
                collector.flush();
            } else {
                markers[pindex].add(pendings[pindex]);
            }
        }
    }

    private RecoverEntry start(Entry entry, int pindex) {
        synchronized(markers[pindex]) {
            if (pendings[pindex] == null) {
                pendings[pindex] = new RecoverEntry(entry);
                desync = true;
            }
            return pendings[pindex];
        }
    }

    private RecoverEntry end(Entry entry, int pindex) {
        synchronized(markers[pindex]) {
            RecoverEntry recover = pendings[pindex];
            if (recover != null) {
                pendings[pindex].end(entry);
                pendings[pindex] = null;
            }
            return recover;
        }
    }

    public int[] retrieve(byte[] key, int hash) {
        int partition = namespace.partitionIDForHash(hash);
        System.out.println("-- [PartitionedCache/retrieve] " + new String(key) + " --> partition = " + partition);
        elements[partition].awaitRecovery(SYNC_TIMEOUT);
        return elements[partition].retrieve(key, hash);
    }

    private boolean replay(int[] mindex, int[] findex) {
        int[] information = records.information(mindex);

        int server = Generations.SERVER(information);
        long counter = Generations.COUNTER(information);

        int partition = namespace.partitionIDForHash(information[Constants.INFO_HASH]);
        if (namespace.partitionIndexFor(partition, server) < 0) {
            return false;
        }
        int pindex = namespace.partitionIndexForSelf(partition);
        if (pindex < 0) {
            return false;
        }
        System.out.println("-- [PartitionedCache/recovered] " + records.keyString(mindex) + " --> partition = " + partition);
        elements[partition].store(pindex, kvindex(information, mindex, findex));
        return true;
    }

    private int[][] kvindex(int[] info, int[] mindex, boolean persist) {
        if (persist && storage != null) {
            return kvindex(info, mindex, storage.store(mindex));
        }
        return kvindex(info, mindex);
    }

    private int[][] kvindex(int[] info, int[] mindex, int[] findex) {
        return new int[][]{info, mindex, findex};
    }

    private int[][] kvindex(int[] info, int[] mindex) {
        return new int[][]{info, mindex};
    }

    public void flushAll() {
        for (KVIndexCache partition : elements) {
            partition.flush();
        }
    }

    public int[][] usages() {
        int[][] usage = new int[namespace.partitionNum()][2];
        for (int i = 0; i < usage.length; i++) {
            usage[i][0] = elements[i].usageKB();
            usage[i][1] = elements[i].remainKB();
        }
        return usage;
    }

    public long[][] snapshot() {
        return snapshots;
    }

    public long[] snapshot(int partition) {
        return snapshots[partition];
    }

    // sync data for sync request
    public void response(int[][] requests, DataCollector collector) {
        for (int[] request : requests) {
            int sindex = Generations.SERVER(request);
            int partition = Generations.HASH(request);
            long counter = Generations.COUNTER(request);
            System.out.println("[PartitionedCache/response] " + partition + "::" + sindex + ":" + counter);
            elements[partition].search(sindex, counter, collector);
            collector.completed(partition);
        }
    }

    // sync request for snapshot
    public void request(long[][] snapshot, SyncRequests request) {
        List<int[]> requests = new ArrayList<int[]>();
        for (int partition = 0; partition < snapshot.length; partition++) {
            for (int pindex = 0; pindex < snapshot[partition].length; pindex++) {
                long generation = recoveryTarget(partition, pindex, snapshot[partition][pindex]);
                if (generation >= 0) {
                    int sindex = namespace.partitionFor(partition, pindex);
                    System.out.println("[PartitionedCache/request] " + partition + "::" + sindex + ":" + generation + "~" + generationFor(partition, pindex));
                    requests.add(Generations.INDEX(sindex, generation, partition));
                }
            }
        }
        request.add(namespace.index(), requests);
    }

    public long generationFor(int partition, int pindex) {
        return elements[partition].generations[pindex];
    }

    public long recoveryTarget(int partition, int pindex, long generation) {
        long start = elements[partition].recovering(pindex, generation);
        if (start > 0) {
            System.out.println("[PartitionedCache/shouldRecover] acquire recovery lock for " + partition + "::" + pindex + ":" + generation);
        }
        return start;
    }

    public boolean recovered(int partition) {
        boolean completed = elements[partition].recovered();
        System.out.println("[PartitionedCache/shouldRecover] release recovery lock for " + partition + ", completed = " + completed);
        return completed;
    }

    private class ExpireManager implements Runnable {

        private static final int EXPIRE_INTERVAL = 60000;
        private static final int EXPIRE_PER_ITERATION = 100;

        public void run() {
            while (resource.isActive()) {
                try {
                    Thread.sleep(EXPIRE_INTERVAL);
                } catch (InterruptedException e) {
                    continue;
                }
                for (KVIndexCache cache : elements) {
                    cache.expire(EXPIRE_PER_ITERATION);
                }
            }
        }
    }

    private static class ForwardResolver implements LinkedSet.ConflictResolver<int[]> {

        public boolean override(int[] expected, int[] current) {
            return !Generations.SAME_SERVER(expected, current) || Generations.OLDER(expected, current);
        }
    }

    private static class SyncResolver implements LinkedSet.ConflictResolver<int[]> {

        private final int partition;
        private final NameSpace namespace;

        public SyncResolver(NameSpace namespace, int partition) {
            this.namespace = namespace;
            this.partition = partition;
        }

        public boolean override(int[] expected, int[] current) {
            int pindex1 = namespace.partitionIndexFor(partition, Generations.SERVER(expected));
            int pindex2 = namespace.partitionIndexFor(partition, Generations.SERVER(current));
            if (pindex2 >= 0 && pindex1 == pindex2) {
                return Generations.COUNTER(expected) > Generations.COUNTER(current);
            }
            return pindex2 < 0 || pindex1 < pindex2;
        }
    }

    private static class RecoverEntry implements Iterable<Entry<int[]>> {

        Entry start;
        Entry end;

        RecoverEntry(LinkedSet.Entry start) {
            this.start = start;
        }

        void end(LinkedSet.Entry end) {
            this.end = end;
        }

        public Iterator<Entry<int[]>> iterator() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }
    }
}
