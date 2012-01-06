package com.nexr.cache;

public interface GenerationManager {
    
    long generation(int namespace, int partition, int pindex);

    long recoverable(int namespace, int partition, int pindex, long generation);
}
