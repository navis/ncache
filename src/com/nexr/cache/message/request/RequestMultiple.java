package com.nexr.cache.message.request;

import java.util.LinkedList;

import com.nexr.cache.message.Message;

public class RequestMultiple extends AbstractRequest {

    int length;
    int counter;
    LinkedList<Message> messages;

    public RequestMultiple(int callid, int length) {
        super(callid);
        this.length = length;
        this.messages = new LinkedList<Message>();
    }

    protected boolean isArrived() {
        return !messages.isEmpty();
    }

    protected Message result() {
        return messages.pollFirst();
    }

    public synchronized void arrived(Message message) {
        counter++;
        messages.push(message);
        notifyAll();
    }

    public void skipped(int partition) {
        counter++;
    }

    public boolean remaining() {
        return counter < length;
    }
}
