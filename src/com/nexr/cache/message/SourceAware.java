package com.nexr.cache.message;

import com.nexr.cache.ServerEntity;

public interface SourceAware {

    int index();

    ServerEntity source();

    void source(ServerEntity source);
}
