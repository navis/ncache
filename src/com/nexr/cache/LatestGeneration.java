package com.nexr.cache;

public class LatestGeneration {

    int[] latest;

    public LatestGeneration(int length) {
        latest = new int[length];
    }

    private synchronized void set(int server, int counter) {
        reserve(server);
        latest[server] = Math.max(latest[server], counter);
    }

    private void reserve(int expected) {
        if (expected >= latest.length) {
            int[] newArray = new int[Math.max(expected, latest.length + 32)];
            System.arraycopy(latest, 0, newArray, 0, latest.length);
            latest = newArray;
        }
    }
}
