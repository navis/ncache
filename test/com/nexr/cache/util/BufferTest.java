package com.nexr.cache.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.EOFException;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;

import sun.misc.VM;
import com.nexr.cache.develop.ByteBufferPool;
import com.nexr.cache.MemoryPool;

public class BufferTest {

    static int THREAD = 1;

    static int SPLIT_COUNT = 128 << 10;
    static int SPLIT_SIZE = 1 << 18;

    static int POOL_SIZE = 6 << 20;

    public static void main(String[] args) throws IOException, InterruptedException {
        if (!new File(args[0]).exists()) {
            throw new IllegalArgumentException(args[0]);
        }
        final BlockingQueue<int[]> queue = new LinkedBlockingQueue<int[]>();

        final Random random = new Random();
        final RandomAccessFile input = new RandomAccessFile(args[0], "r");
        long length = input.length() - SPLIT_SIZE;
        int increment = SPLIT_COUNT < 0 ? SPLIT_SIZE : (int) (length / SPLIT_COUNT);
        for (int i = 0; i < length; i += increment) {
//            queue.add(new long[]{i, Math.min(length - i, SPLIT_SIZE)});
            queue.add(new int[]{i, random.nextInt(SPLIT_SIZE / 4) + (SPLIT_SIZE / 4  * 3)});
        }
        System.out.println("[Test/main] pool = " + (POOL_SIZE >> 10)+ "KB, work = " + queue.size() + ", DM = " + VM.maxDirectMemory());

        final FileChannel channel = input.getChannel();

        final CyclicBarrier start = new CyclicBarrier(THREAD);
        final CountDownLatch finish = new CountDownLatch(THREAD);

        final ByteBufferPool buffer = new ByteBufferPool(POOL_SIZE, true);
        final MemoryPool pool = buffer;
//        final Cache pool = new MemcachedWrapper(buffer, 17, 2);

        for (int i = 0; i < THREAD; i++) {
            Thread thread = new Thread(new Runnable() {
                int counter;
                public void run() {
                    try {
                        start.await();
                        while (true) {
                            int[] indexes = queue.poll();
                            if (indexes == null) {
                                break;
                            }
                            int[] index = pool.poll(indexes[1]);
                            ByteBuffer buffer = pool.regionFor(index, true);
//                            System.out.println("[Test/run] " + indexes[1] + " = " + Arrays.toString(index));
                            try {
                                int total = 0;
                                do {
                                    int read = channel.read(buffer, indexes[0] + total);
                                    if (read < 0) {
                                        throw new EOFException("read = " + read + ", expected " + indexes[1] + ", buffer " + buffer);
                                    }
                                    total += read;
                                } while (total != indexes[1]);
                            } finally {
                                pool.release(index);
                            }
//                            channel.read(ByteBuffer.allocate(indexes[1]), indexes[0]);
//                            channel.read(ByteBuffer.allocateDirect((int) indexes[1]), indexes[0]);
                            counter++;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally{
                        finish.countDown();
                        System.out.println(Thread.currentThread().getName() + " = " + counter);
                    }
                }
            });
//            thread.setPriority(Thread.MIN_PRIORITY);
            thread.start();
        }
        long prev = System.currentTimeMillis();
        finish.await();

        System.out.println("[Test/main] " + (System.currentTimeMillis() - prev) + " msec");
        System.out.println(pool);
    }
}
