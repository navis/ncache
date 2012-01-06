package com.nexr.cache.util;

import java.io.File;
import java.io.IOException;

import static com.nexr.cache.util.Property.*;
import com.nexr.cache.MemcachedMPool;
import com.nexr.cache.MemcachedFPool;
import com.nexr.cache.RecordManager;
import com.nexr.cache.PersistentPool;
import com.nexr.cache.Region;

public class RecordDump {

    public static void main(String[] args) throws IOException {
        MemcachedMPool pool = new MemcachedMPool();
        pool.initialize(MPOOL_START_DEFAULT, MPOOL_INCREASE_DEFAULT, MPOOL_SLABSIZE_DEFAULT);

        MemcachedFPool fpool = new MemcachedFPool(pool, new File(args[0]));
        fpool.initialize(PPOOL_START_DEFAULT, PPOOL_INCREASE_DEFAULT, PPOOL_SLABSIZE_DEFAULT);

        String namespace = args.length > 1 ? args[1] : "default";

        final RecordManager records = new RecordManager(pool);
        fpool.initialize(new PersistentPool.Initializer() {
            public boolean compact() {
                return false;
            }
            public void keyvalue(int[] mindex, int[] pindex) {
                System.out.println("-- [RecordDump/keyvalue] " + Region.toString(mindex) + " --> " + records.keyString(mindex));
            }
        });
    }
}
