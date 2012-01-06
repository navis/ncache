package com.nexr.cache;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class NameSpace implements Comparable<NameSpace> {

    protected int[][] partitions; // pindex --> sindex[]

    protected byte index;
    protected String name;

    protected int limitKB;
    protected int replica;
    protected int partition;
    protected boolean persist;

    public NameSpace(byte index, String name, int limitKB, int replica, int partition, boolean persist) {
        this.index = index;
        this.name = name;
        this.limitKB = limitKB;
        this.replica = replica;
        this.partition =  partition;
        this.persist = persist;
    }

    public String name() {
        return name;
    }

    public byte index() {
        return index;
    }

    public int partitionNum() {
        return partition;
    }

    public int replicaNum() {
        return replica;
    }

    public int limitKB() {
        return limitKB;
    }

    public PersistentPool storage() {
        return null;
    }

    public void destroy() {
    }

    public synchronized int[][] partitions() {
        return partitions;
    }

    public synchronized int partitionIDForHash(int hash) {
        return Math.abs(hash) % partitions.length;
    }

    public synchronized int[] partitionForHash(int hash) {
        return partitions[partitionIDForHash(hash)];
    }

    public synchronized int[] partitionFor(int partitionID) {
        return partitions[partitionID];
    }

    public synchronized int partitionFor(int partitionID, int pindex) {
        return partitions[partitionID][pindex];
    }

    public synchronized void syncPartition(int[][] update) {
        this.partitions = update;
    }

    public int partitionIndexFor(int partitionID, int sindex) {
        return findIndex(partitionID, sindex);
    }

    public int partitionIndexForSelf(int partitionID) {
        return findIndex(partitionID, index);
    }

    public boolean isPrevious(int partitionID, int sindex) {
        return sindex == partitionFor(partitionID, partitionIndexForSelf(partitionID) - 1);
    }

    protected int findIndex(int partitionID, int sindex) {
        int index = 0;
        for (int psindex : partitionFor(partitionID)) {
            if (psindex == sindex) {
                return index;
            }
            index++;
        }
        return -1;
    }

    public <NS extends NameSpace> NS update(NS namespace) {
        throw new UnsupportedOperationException("update");
    }

    public int hashCode() {
        return index;
    }

    public boolean equals(Object obj) {
        NameSpace other = (NameSpace) obj;
        return index == other.index && name.equals(name) && limitKB == other.limitKB && replica == other.replica && partition == other.partition && persist == other.persist;
    }

    public int compareTo(NameSpace o) {
        return index - o.index;
    }

    public String serialize() {
        return index + ":" + name + ":" + limitKB +":" + replica + ":" + partition + ":" + persist;
    }

    public void serialize(DataOutputStream output) throws IOException {
        output.writeByte(index);
        output.writeUTF(name);
        output.writeInt(limitKB);
        output.writeInt(replica);
        output.writeInt(partition);
        output.writeBoolean(persist);
    }

    protected int[] indexesFor(int[][] update, int target) {
        int[] indexes = new int[update.length];
        Arrays.fill(indexes, -1);
        for (int i = 0; i < update.length; i++) {
            for (int index = 0; index < update[i].length; index++) {
                if (update[i][index] == target) {
                    indexes[i] = index;
                    break;
                }
            }
        }
        return indexes;
    }

    public static <NS extends NameSpace>NS valueOf(NameSpaceManager<NS> nspace, String value) {
        String[] split = value.split(":");
        byte index = Byte.valueOf(split[0]);
        String name = split[1];
        int limitKB = Integer.valueOf(split[2]);
        int replica = Integer.valueOf(split[3]);
        int partition = Integer.valueOf(split[4]);
        boolean persist = Boolean.valueOf(split[5]);
        return nspace.newInstance(index, name, limitKB, replica, partition, persist);
    }

    public static NameSpace defaultNameSpace() {
        return new NameSpace((byte) 0, "default", 64 << 10, 3, 271, false);
    }
}
