package com.nexr.cache.message.request;

import com.nexr.cache.message.MessageListener;
import com.nexr.cache.message.Message;
import com.nexr.cache.message.Ack;

public class RequestAggregation extends AbstractRequest {

    transient MessageListener listener;

    int request;
    int accept;

    public RequestAggregation(int callid, MessageListener listener) {
        super(callid);
        this.listener = listener;
    }

    public int awaitingOn() {
        return request;
    }

    public synchronized void increment() {
        request++;
    }

    protected synchronized boolean isArrived() {
        return accept >= request;
    }

    protected Message result() {
        return isArrived() ? new Ack() : null;
    }

    public synchronized void arrived(Message message) {
        if (listener != null) {
            listener.arrived(message);
        }
        accept++;
        notifyAll();
    }
}
