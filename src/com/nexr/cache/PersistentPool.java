package com.nexr.cache;

import java.io.IOException;
import java.nio.channels.FileChannel;

public interface PersistentPool extends RegionPool<FileChannel> {

    void initialize(Initializer init) throws IOException;

    void synchronize(FileChannel channel) throws IOException;

    int[] store(int[] mindex);

    int[] remove(int[] pindex);

    interface Initializer {
        boolean compact();
        void keyvalue(int[] mindex, int[] pindex);
    }
}
