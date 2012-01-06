package com.nexr.cache;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

public class IndexFilesTest {

    @Test
    public void test() throws IOException {
        MemcachedMPool pool = new MemcachedMPool();
        pool.initialize(48, 0.25f, 4 << 10);

        IndexFiles file = new IndexFiles(new File("tmp"), pool);
        file.intialize(true);
        System.out.println(file.dump());

        int[] index1 = new int[] {0, 100, 100};
        int[] index2 = new int[] {0, 200, 200};
        int[] index3 = new int[] {1, 150, 150};
        int[] index4 = new int[] {1, 250, 250};

        file.put(index1);
        System.out.println(file.dump());
        file.put(index3);
        System.out.println(file.dump());
        file.remove(index1);
        System.out.println(file.dump());
        file.put(index4);
        System.out.println(file.dump());

        file.compact();
        file.put(index2);
        System.out.println(file.dump());
    }
}
