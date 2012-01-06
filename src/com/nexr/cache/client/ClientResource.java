package com.nexr.cache.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.TimerTask;
import java.util.Timer;

import com.nexr.cache.MemoryPool;
import com.nexr.cache.ResourceManager;
import com.nexr.cache.util.Property;

public class ClientResource implements ResourceManager {

    Property property;

    Timer timer;
    MemoryPool pool;
    ExecutorService executor;
    AtomicInteger counter;

    public ClientResource() {
        initialize(null);
    }

    public boolean isActive() {
        return executor != null && !executor.isShutdown();
    }

    public void initialize(Property property) {
        pool = new DummyPool();
        timer = new Timer(true);
        executor = new SimpleExecutor();
        counter = new AtomicInteger();
        this.property = property;
    }

    public Property property() {
        return property;
    }

    public MemoryPool memory() {
        return pool;
    }

    public void execute(Runnable runnable) {
        executor.execute(runnable);
    }

    public void schedule(TimerTask task, long delay) {
        timer.schedule(task, delay);
    }

    public void schedule(TimerTask task, long delay, long interval) {
        timer.schedule(task, delay, interval);
    }

    public int nextCallID() {
        return counter.incrementAndGet();
    }

    public void destroy() {
        timer.cancel();
        executor.shutdownNow();
    }

    private static class DaemonThreadFactory implements ThreadFactory {

        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            return thread;
        }
    }
}
