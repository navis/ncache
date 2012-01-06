package com.nexr.cache;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.nexr.cache.util.Property;
import com.nexr.cache.ServerConnectionManager;

public class CacheServerResource implements ServerResource {

    Property property;

    Timer timer;
    MemoryPool memory;
    ExecutorService executor;
    AtomicInteger counter;
    AtomicInteger client;

    MessageHandler service;
    ServerConnectionManager cluster;

    public boolean isActive() {
        return executor != null && !executor.isShutdown();
    }

    public void initialize(Property props) throws Exception {
        memory = initializeMemory(props);
        executor = initializeExecutor(props);
        counter = new AtomicInteger();
        client = new AtomicInteger();
        timer = new Timer(true);
        property = props;
    }

    public void initialized(ServerConnectionManager cluster, MessageHandler service) {
        this.cluster = cluster;
        this.service = service;
    }

    public Property property() {
        return property;
    }

    public ServerConnectionManager cluster() {
        return cluster;
    }

    public MessageHandler service() {
        return service;
    }

    public int clientID() {
        return client.incrementAndGet();
    }

    protected MemoryPool initializeMemory(Property props) throws IOException {
        int start = props.getInt(Property.MPOOL_START);
        float increment = props.getFloat(Property.MPOOL_INCREASE);
        int slabsize = props.getInt(Property.MPOOL_SLABSIZE);
        MemcachedMPool pool = new MemcachedMPool();
        pool.initialize(start, increment, slabsize);
        return pool;
    }

    protected ExecutorService initializeExecutor(Property props) {
        int threads = props.getInt(Property.THREAD_POOL);
        return Executors.newFixedThreadPool(threads);
    }

    public MemoryPool memory() {
        return memory;
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
        cluster.destroy();
        executor.shutdownNow();
    }
}
