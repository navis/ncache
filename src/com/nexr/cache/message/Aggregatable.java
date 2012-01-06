package com.nexr.cache.message;

import com.nexr.cache.message.request.Request;

public interface Aggregatable {

    boolean isFinished(Request request);
}
