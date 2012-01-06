package com.nexr.cache.develop;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import com.nexr.cache.Utils;

public class MapElement {

    volatile Map<Key, Value> cache;

    public MapElement() {
        this.cache = new LinkedHashMap<Key, Value>();
    }

    public Value retrieve(byte[] key) {
        return _get(key, Utils.hash(key));
    }

    public Value retrieve(byte[] key, int hash) {
        return _get(key, hash);
    }

    public Value store(byte[] bkey, int hash, int expire, int[] vindex) {
        Key key = new Key(bkey, hash);
        Value value = new Value(vindex, expire);

        return _put(key, value);
    }

    public Value store(byte[] bkey, int expire, int[] vindex) {
        Key key = new Key(bkey, Utils.hash(bkey));
        Value value = new Value(vindex, expire);

        return _put(key, value);
    }

    private Value _put(Key key, Value value) {
        Map<Key, Value> access = cache;
        synchronized (access) {
            return access.put(key, value);
        }
    }

    private Value _get(byte[] key, int hash) {
        Map<Key, Value> access = cache;
        synchronized (access) {
            return access.get(new Key(key, hash));
        }
    }

    public Map<Key, Value> clear() {
        Map<Key, Value> original = cache;
        cache = new LinkedHashMap<Key, Value>();
        return original;
    }

    private static class Key {

        public byte[] value;
        public int hash;

        public Key(byte[] value, int hash) {
            this.value = value;
            this.hash = hash;
        }

        public int hashCode() {
            return hash;
        }

        public boolean equals(Object obj) {
            Key other = (Key) obj;
            return Arrays.equals(value, other.value);
        }
    }

    public static class Value {

        public int[] vindex;
        public int expire;

        public Value(int[] vindex, int expire) {
            this.vindex = vindex;
            this.expire = expire;
        }
    }
}
