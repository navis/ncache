package com.nexr.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.nio.ByteBuffer;

import org.junit.Test;

import junit.framework.Assert;
import com.nexr.cache.develop.SimpleMemoryPool;

public class SimpleMemoryPoolTest {

    Random random = new Random();

    @Test
    public void testBasic() {
        SimpleMemoryPool.MemorySlab slab = new SimpleMemoryPool.MemorySlab(0, ByteBuffer.allocateDirect(32 << 20));
        int[] index = slab.reserve(1024);
        slab.release(index);
    }

    @Test
    public void testCompact() {
        SimpleMemoryPool.MemorySlab slab = new SimpleMemoryPool.MemorySlab(0, ByteBuffer.allocateDirect(32 << 20));
        int[] index1 = slab.reserve(2 << 8);
        int[] index2 = slab.reserve(2 << 9);
        int[] index3 = slab.reserve(2 << 10);
        int[] index4 = slab.reserve(2 << 11);
        int[] index5 = slab.reserve(2 << 12);
        slab.release(index1);
        slab.release(index2);
        slab.release(index4);
        slab.compact();

        Assert.assertEquals(slab.remove(index3)[2], 2 << 10);
        Assert.assertEquals(slab.remove(index5)[2], 2 << 12);
    }

    @Test
    public void test2() throws Exception {

        final int THREAD = 16;
//        final SimpleMemoryPool list = new SimpleMemoryPool();
//        list.start();
        final MemcachedMPool list = new MemcachedMPool();
        list.initialize(128, 0.25f, 4 << 10);

        final CyclicBarrier start = new CyclicBarrier(THREAD);
        final CountDownLatch latch = new CountDownLatch(THREAD);
        final AtomicBoolean finish = new AtomicBoolean(false);

        for (int i = 0; i < THREAD; i++) {
            Thread thread = new Thread(new Runnable() {
                int allocate;
                int release;
                List<int[]> indexes = new ArrayList<int[]>();

                public void run() {
                    try {
                        start.await();
                        while (!finish.get()) {
                            if (indexes.isEmpty() || (indexes.size() < random.nextInt(2048))) {
                                int[] index = list.allocate(random.nextInt(4096) + 32);
                                if (index == null) {
                                    throw new IllegalStateException();
                                }
//                                System.out.println("[MappingListTest/run] reserved " + list.toString(index));
//                                if (random.nextFloat() > 0.001f) {
                                    indexes.add(index);
                                allocate++;
//                                }
                            } else {
                                int[] index = indexes.remove(random.nextInt(indexes.size()));
//                                System.out.println("[MappingListTest/run] releasing " + index);
                                list.release(index);
                                release++;
                            }
                            Thread.yield();
//                            Thread.sleep(10);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(1);
                    } finally {
                        System.out.println("[MappingListTest/run] " + Thread.currentThread() + " --> " + allocate + ":" + release);
//                        System.out.println("-- [SimpleMemoryPoolTest/run] " + list.heap);
                        latch.countDown();
                    }
                }
            });
            thread.start();
        }
        latch.await(6000, TimeUnit.MILLISECONDS);
//        long prev = System.currentTimeMillis();
        finish.set(true);
        latch.await();

//        System.out.println("[Test/main] " + (System.currentTimeMillis() - prev) + " msec");
    }

    public static void main(String[] args) throws Exception {
        SimpleMemoryPoolTest test = new SimpleMemoryPoolTest();
        test.test2();
    }
}
