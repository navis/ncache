package com.nexr.cache.message.request;

import com.nexr.cache.message.Message;

public abstract class AbstractRequest implements Request {

//    private static final int DEFAULT_POLLING = 3000;

    final int callid;

    public AbstractRequest(int callid) {
        this.callid = callid;
    }

    public int callid() {
        return callid;
    }

    public synchronized Message await() {
        try {
            while (!isArrived()) {
                wait();
            }
        } catch (InterruptedException e) {
            // ignore
        }
        return result();
    }

    public synchronized Message await(long timeout) {
        long remaining = timeout;
        try {
            long prev = System.currentTimeMillis();
            while (!isArrived() && remaining > 0) {
                wait(remaining);
                long current = System.currentTimeMillis();
                remaining -= current - prev;
                prev = current;
            }
        } catch (InterruptedException e) {
            // ignore
        }
        return result();
    }

    public boolean arrived() {
        return isArrived();
    }

    protected abstract boolean isArrived();

    protected abstract Message result();
}
