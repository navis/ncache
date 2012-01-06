package com.nexr.cache.message;

import java.nio.ByteBuffer;

public abstract class AbstractDeserializable implements Deserializable {
    public int header() {
        return 0;
    }
    public void prepare(ByteBuffer buffer) {
        buffer.position(header());
    }
}
