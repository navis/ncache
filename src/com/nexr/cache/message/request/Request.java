package com.nexr.cache.message.request;

import com.nexr.cache.message.Message;

public interface Request {
    int callid();
    Message await();
    Message await(long timeout);
    void arrived(Message message);
    boolean arrived();
}
