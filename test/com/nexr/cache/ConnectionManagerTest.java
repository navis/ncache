package com.nexr.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import junit.framework.Assert;
import com.nexr.cache.client.ServiceHandler;

public class ConnectionManagerTest {

    Random random = new Random();

    @Test
    public void testBasic() throws Exception {
        ServiceHandler manager = new ServiceHandler("navis", "127.0.0.1:2181");
        manager.initialize();

        String key = "jlxqleleiduo";
        byte[] value = "navis manse".getBytes();
        try {
            manager.remove(key);
            Assert.assertNull(manager.get(key));

            manager.put(key, value);
            Assert.assertTrue(Arrays.equals(value, manager.get(key)));

            Assert.assertTrue(manager.remove(key));
            Assert.assertNull(manager.get(key));
        } finally {
            manager.shutdown();
        }
    }

    @Test
    public void testReplicate() throws Exception {
        ServiceHandler manager = new ServiceHandler("navis", "127.0.0.1:2181");
        manager.initialize();

        String key = "jlxqleleiduo";
        byte[] value = "navis manse".getBytes();
        System.out.println("[ConnectionManagerTest/testReplicate] " + Utils.hash(key.getBytes()));
        try {
            manager.remove(key);
            Assert.assertNull(manager.get(key));

            manager.put(key, value);
            Assert.assertTrue(Arrays.equals(value, manager.get(key)));

            Assert.assertTrue(manager.remove(key));
            Assert.assertNull(manager.get(key));
        } finally {
            manager.shutdown();
        }
    }

    @Test
    public void testExpire() throws Exception {
        ServiceHandler manager = new ServiceHandler("navis", "127.0.0.1:2181");
        manager.initialize();

        String key = "navis";
        byte[] value = "navis manse".getBytes();
        System.out.println("[ConnectionManagerTest/testExpire] " + Utils.hash(key.getBytes()));
        try {
            manager.remove(key);
            Assert.assertNull(manager.get(key));

            manager.put(key, value, 3);
            Assert.assertTrue(Arrays.equals(value, manager.get(key)));

            Thread.sleep(3000);

            Assert.assertNull(manager.get(key));
        } finally {
            manager.shutdown();
        }
    }

    @Test
    public void testRush() throws Exception {
        int packet = 128;
        int damper = 256;
        int iteration = 100000;

        boolean persist = true;

        ServiceHandler manager = new ServiceHandler("navis", "127.0.0.1:2181");
        manager.initialize();
        manager.flushAll();

        long start = System.currentTimeMillis();

        int put = 0;
        int remove = 0;
        int update = 0;
        List<String> keys = new ArrayList<String>();
        for (int i = 0; i < iteration; i++) {
            if (keys.isEmpty() || (keys.size() < random.nextInt(damper))) {
                String key = randomStr(10);
//                System.out.println("[ConnectionManagerTest/testPersistency] put " + key);
                if (!manager.put(key, new byte[packet], persist)) {
                    throw new IllegalStateException(key);
                }
                keys.add(key);
                put++;
            } else {
                String key = keys.remove(random.nextInt(keys.size()));
//                System.out.println("[ConnectionManagerTest/testPersistency] remove " + key);
                if (random.nextBoolean()) {
                    if (!manager.put(key, new byte[packet], persist)) {
                        throw new IllegalStateException(key);
                    }
                    update++;
                } else {
                    if (!manager.remove(key)) {
                        throw new IllegalStateException(key);
                    }
                    remove++;
                }
            }
        }
        System.out.println("-- [ConnectionManagerTest/testRush] " + (System.currentTimeMillis() - start)  + " msec, " + put + ":" + update + ":" + remove);
//        System.out.println(keys.toString().replaceAll(", ", "\r\n"));

        manager.shutdown();
    }

    @Test
    public void test() throws Exception {

        final int THREAD = 1;

        final ServiceHandler manager = new ServiceHandler("navis", "127.0.0.1:2181");
        manager.initialize();

        final CyclicBarrier start = new CyclicBarrier(THREAD);
        final CountDownLatch latch = new CountDownLatch(THREAD);
        final AtomicBoolean finish = new AtomicBoolean(false);

        for (int i = 0; i < THREAD; i++) {
            Thread thread = new Thread(new Runnable() {

                int allocate;
                int release;
                List<String> keys = new ArrayList<String>();

                public void run() {
                    try {
                        start.await();
                        while (!finish.get()) {
                            if (keys.isEmpty() || (keys.size() < random.nextInt(1024))) {
                                String key = randomStr(12);
                                if (!manager.put(key, new byte[random.nextInt(4096) + 32], true)) {
                                    throw new IllegalStateException(key);
                                }
                                keys.add(key);
                                allocate++;
                            } else {
                                String key = keys.remove(random.nextInt(keys.size()));
                                if (random.nextBoolean()) {
                                    if (!manager.put(key, new byte[random.nextInt(4096) + 32])) {
                                        throw new IllegalStateException(key);
                                    }
                                } else {
                                    if (!manager.remove(key)) {
                                        throw new IllegalStateException(key);
                                    }
                                }
                                release++;
                            }
                            Thread.yield();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        finish.set(true);
                    } finally {
                        System.out.println("[MappingListTest/run] " + Thread.currentThread() + " --> " + allocate + ":" + release);
                        latch.countDown();
                    }
                }
            });
            thread.start();
        }

        latch.await(10000, TimeUnit.MILLISECONDS);

        finish.set(true);
        latch.await();

        manager.shutdown();
    }

    private String randomStr(int length) {
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append((char) ('a' + random.nextInt(26)));
        }
        return builder.toString();
    }
}
