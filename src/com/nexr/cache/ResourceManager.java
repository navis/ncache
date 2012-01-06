package com.nexr.cache;

import java.util.TimerTask;

import com.nexr.cache.util.Property;

public interface ResourceManager {

    boolean isActive();

    void initialize(Property props) throws Exception;

    void destroy();

    Property property();

    MemoryPool memory();

    void execute(Runnable runnable);

    void schedule(TimerTask task, long delay);

    void schedule(TimerTask task, long delay, long interval);

    int nextCallID();
}
