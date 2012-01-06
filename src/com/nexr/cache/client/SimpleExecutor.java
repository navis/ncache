package com.nexr.cache.client;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

public class SimpleExecutor extends AbstractExecutorService {

    boolean shutdown;

    public void shutdown() {
        shutdown = true;
    }

    public List<Runnable> shutdownNow() {
        return Collections.emptyList();
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public boolean isTerminated() {
        return shutdown;
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return true;
    }

    public void execute(Runnable command) {
        Thread thread = new Thread(command);
        thread.setDaemon(true);
        thread.start();
    }
}
