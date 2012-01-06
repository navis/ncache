package com.nexr.cache;

import java.util.ArrayList;
import java.util.List;

import com.nexr.cache.ServerEntity;
import com.nexr.cache.message.SyncData;
import com.nexr.cache.message.SyncCompleted;

public class DataCollector {

    private static final int THRESHOLD = 256 << 10;

    byte namespace;
    ServerEntity source;

    int buffered;
    List<int[]> indexes;

    public DataCollector(byte namespace, ServerEntity source) {
        this.namespace = namespace;
        this.source = source;
        this.indexes = new ArrayList<int[]>();
    }

    public int index() {
        return source.index();
    }

    public void push(int[] mindex) {
        if (buffered > THRESHOLD) {
            flush();
        }
        indexes.add(mindex);
        buffered += Region.toLength(mindex);
    }

    public void flush() {
        if (!indexes.isEmpty()) {
            source.notify(new SyncData(namespace, indexes.toArray(new int[indexes.size()][])));
            indexes.clear();
            buffered = 0;
        }
    }

    public void completed(int partition) {
        source.notify(new SyncCompleted(namespace, partition));
    }
}
