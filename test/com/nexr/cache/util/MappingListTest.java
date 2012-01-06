package com.nexr.cache.util;

import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.nexr.cache.develop.MappingList;
import com.nexr.cache.develop.IndexNode;

public class MappingListTest {

    Random random = new Random();

    @Test
    public void testAdjust() throws Exception {
        MappingList list = new MappingList();
        list.addFree(new int[]{0, 40});
        list.addFree(new int[]{40, 60});
        System.out.println(list);
        IndexNode index1 = list.reserve(50);
        System.out.println(list);
        list.release(index1);
        System.out.println(list);
    }

    @Test
    public void testMerge() throws Exception {
        final MappingList list = new MappingList(1024);

        IndexNode[] indexes = new IndexNode[6];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = list.reserve(30 + i);
        }
        list.release(indexes[1]);
        list.release(indexes[3]);
        list.release(indexes[4]);
        list.reserve(60);
    }

    @Test
    public void test() throws Exception {
        final MappingList list = new MappingList(1024);

        Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                public void run() {
                    IndexNode index;
                    do {
                        index = list.reserve(random.nextInt(32));
                        System.out.println("[MappingListTest/run] " + index);
                    } while (index != null);
                }
            };
        }
        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        System.out.println("[MappingListTest/test] " + list);
    }

    @Test
    public void test2() throws Exception {

        final int THREAD = 1;
        final MappingList list = new MappingList(32 << 20);

        final CyclicBarrier start = new CyclicBarrier(THREAD);
        final CountDownLatch latch = new CountDownLatch(THREAD);
        final AtomicBoolean finish = new AtomicBoolean(false);

        for (int i = 0; i < THREAD; i++) {
            Thread thread = new Thread(new Runnable() {
                int counter;
                List<IndexNode> indexes = new ArrayList<IndexNode>();

                public void run() {
                    try {
                        start.await();
                        while (!finish.get()) {
                            if (indexes.isEmpty() || (indexes.size() < random.nextInt(10))) {
                                IndexNode index = list.reserve(100);
                                if (index == null) {
                                    throw new IllegalStateException();
                                }
                                System.out.println("[MappingListTest/run] reserved " + index);
                                indexes.add(index);
                            } else {
                                IndexNode index = indexes.remove(random.nextInt(indexes.size()));
                                if (index == null) {
                                    throw new IllegalStateException();
                                }
                                System.out.println("[MappingListTest/run] releasing " + index);
                                list.release(index);
                            }
                            counter++;
                            Thread.yield();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(1);
                    } finally {
                        latch.countDown();
                        System.out.println("[MappingListTest/run] " + Thread.currentThread() + " --> " + counter);
                    }
                }
            });
            thread.start();
        }
        latch.await(4000, TimeUnit.MILLISECONDS);
//        long prev = System.currentTimeMillis();
        finish.set(true);
        latch.await();

//        System.out.println("[Test/main] " + (System.currentTimeMillis() - prev) + " msec");
    }
}
