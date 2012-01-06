package com.nexr.cache.client;

import java.io.IOException;

public interface CacheClient {

    int DEFAULT_TIMEOUT = 300;
    int DEAULT_GET_FINDNEXT = 50;
    int DEAULT_PUT_FINDNEXT = 100;

    int DEFAULT_EXPIRE = 0;
    boolean DEFAULT_PERSIST = false;

    void useNamespace(String namespace);

    byte[] get(String key) throws IOException;

    byte[] get(String key, long timeout) throws IOException;

    boolean put(String key, byte[] value) throws IOException;

    boolean put(String key, byte[] value, int expire) throws IOException;

    boolean put(String key, byte[] value, boolean persist) throws IOException;

    boolean put(String key, byte[] value, int expire, boolean persist, long timeout) throws IOException;

    boolean remove(String key) throws IOException;

    void flushAll() throws IOException;

    void shutdown();
}
