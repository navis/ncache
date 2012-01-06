package com.nexr.cache.message.request;

import com.nexr.cache.message.MessageListener;
import com.nexr.cache.message.Message;

public class ListeningRequest implements Request {

    int callid;
    MessageListener listener;

    public ListeningRequest(int callid, MessageListener listener) {
        this.callid = callid;
        this.listener = listener;
    }

    public int callid() {
        return callid;
    }

    public Message await() {
        throw new UnsupportedOperationException();
    }

    public Message await(long timeout) {
        throw new UnsupportedOperationException();
    }

    public void arrived(Message message) {
        listener.arrived(message);
    }

    public boolean arrived() {
        return true;
    }
}
