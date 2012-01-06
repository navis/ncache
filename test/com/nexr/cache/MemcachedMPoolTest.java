package com.nexr.cache;

import java.util.Arrays;

import org.junit.Test;

import junit.framework.Assert;

public class MemcachedMPoolTest {

    @Test
    public void testIndex() {
        float increment = 0.25f;
        MemcachedMPool pool = new MemcachedMPool();
        pool.initialize(32, increment, 4 << 10);

        Assert.assertEquals(0, pool.rackIndex(32));
        Assert.assertEquals(0, pool.rackIndex((int) (32 * (1 + increment))));
        Assert.assertEquals(1, pool.rackIndex((int) (32 * (1 + increment))) + 1);
        Assert.assertEquals(pool.index.length - 1, pool.rackIndex(Integer.MAX_VALUE));

        for (int i = 32 ; i * 1.2f < Integer.MAX_VALUE; i *= 1.2f ) {
            System.out.println(i + " = " + pool.rackIndex(i) + " " + pool.rackLength(pool.rackIndex(i)));
            System.out.println((i + 1)+ " = " + pool.rackIndex(i + 1) + " " + pool.rackLength(pool.rackIndex(i + 1)));
            int[] index = pool.allocate(i);
            pool.release(index);
        }
    }

    @Test
    public void testBasic() {
        MemcachedMPool pool = new MemcachedMPool();
        pool.initialize(32, 0.25f, 4 << 10);
        int[] index1 = pool.allocate(256);
        int[] index2 = pool.allocate(256);
        System.out.println("[MemcachedPoolTest/testBasic] " + Region.toString(index1));
        System.out.println("[MemcachedPoolTest/testBasic] " + Region.toString(index2));
        Assert.assertNotSame(index1, index2);
        pool.release(index1);
        int[] index3 = pool.allocate(256);
        System.out.println("[MemcachedPoolTest/testBasic] " + Region.toString(index3));
        Assert.assertSame(index1, index3);
    }

    public static void main(String[] args) {
        MemcachedMPool pool = new MemcachedMPool();
        pool.initialize(32, 0.25f, 4 << 10);

        for (int i = 32 ; i * 1.2f < Integer.MAX_VALUE; i *= 1.2f ) {
            System.out.println(i + " = " + pool.rackIndex(i) + " " + pool.rackLength(pool.rackIndex(i)));
            System.out.println((i + 1)+ " = " + pool.rackIndex(i + 1) + " " + pool.rackLength(pool.rackIndex(i + 1)));
            int[] index = pool.allocate(i);
            pool.release(index);
        }
        System.exit(1);

        final int[] index = Region.index(1, 1024, 512);
        System.out.println(Arrays.toString(index));
        System.out.println(index[Region.SLAB_INDEX]);

        int[] v1 = pool.allocate(32);
        System.out.println(Arrays.toString(v1));
        System.out.println(pool);
        int[] v2 = pool.allocate(32);
        System.out.println(Arrays.toString(v2));
        System.out.println(pool);
        int[] v3 = pool.allocate(32);
        System.out.println(Arrays.toString(v3));
        System.out.println(pool);
        System.out.println("------------- start release");
        pool.release(v1);
        System.out.println(pool);
        pool.release(v2);
        System.out.println(pool);
        pool.release(v3);
        System.out.println(pool);
        System.out.println("------------- start allocate");
        int[] v4 = pool.allocate(32);
        System.out.println(Arrays.toString(v4));
        System.out.println(pool);
    }
}
