package com.nexr.cache;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class NameSpaceManager<NS extends NameSpace> {

    protected NS[] indexMap;
    protected Map<String, NS> nameMap;

    public NameSpaceManager() {
        this.nameMap = new HashMap<String, NS>();
        this.indexMap = newArray(0);
    }

    public synchronized void syncNamespace(NS namespace) throws IOException {
        NS prev = nameMap.get(namespace.name());
        if (prev == null) {
            initialize(namespace);
            nameMap.put(namespace.name(), namespace);
        } else if (!prev.equals(namespace)) {
            prev.update(namespace);
        }
        indexMap = indexMap(nameMap.values());
    }

    private NS[] indexMap(Collection<NS> namespaces) {
        int max = -1;
        for (NS namespace : namespaces) {
            max = Math.max(max, namespace.index());
        }
        NS[] newArray = newArray(max + 1);
        for (NS namespace : namespaces) {
            newArray[namespace.index()] = namespace;
        }
        return newArray;
    }

    protected NS[] nspaceArray() {
        return indexMap;
    }

    protected NS initialize(NS namespace) throws IOException {
        return namespace;
    }

    protected NS defaultNS() {
        return namespaceFor(0);
    }

    public synchronized NS namespaceFor(int nspace) {
        NS namespace = indexMap.length > nspace ? indexMap[nspace] : null;
        if (namespace == null) {
            throw new IllegalStateException("invalid namespace index "  + nspace);
        }
        return namespace;
    }

    public synchronized NS namespaceFor(String nspace) {
        NS namespace = nameMap.get(nspace);
        if (namespace == null) {
            throw new IllegalStateException("invalid namespace "  + nspace);
        }
        return namespace;
    }

    @SuppressWarnings("unchecked")
    protected NS[] newArray(int length) {
        return (NS[]) new NameSpace[length];
    }

    @SuppressWarnings("unchecked")
    protected NS newInstance(byte index, String name, int limitKB, int replica, int partition, boolean persist) {
        return (NS) new NameSpace(index, name, limitKB, replica, partition, persist);
    }

    public void syncPartition(int nspace, byte[] data) throws Exception {
        namespaceFor(nspace).syncPartition(PartitionUtil.readPartition(data));
    }

    public void syncPartition(String nspace, byte[] data) throws Exception {
        namespaceFor(nspace).syncPartition(PartitionUtil.readPartition(data));
    }
}