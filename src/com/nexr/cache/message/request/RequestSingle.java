package com.nexr.cache.message.request;

import com.nexr.cache.message.Message;

public class RequestSingle extends AbstractRequest {

    Message message;

    public RequestSingle(int callid) {
        super(callid);
    }

    public synchronized void arrived(Message message) {
        this.message = message;
        notifyAll();
    }

    protected boolean isArrived() {
        return message != null;
    }

    protected Message result() {
        return message;
    }
}
