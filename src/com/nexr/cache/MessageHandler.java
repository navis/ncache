package com.nexr.cache;

import com.nexr.cache.message.Message;

public interface MessageHandler {

    Message handle(Message request);
}
