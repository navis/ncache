package com.nexr.cache.message;

public interface Multiparted<T extends Deserializable> {

    int hasNext();

    T next();
}
