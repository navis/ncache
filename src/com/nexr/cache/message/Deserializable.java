package com.nexr.cache.message;

import java.nio.ByteBuffer;
import java.io.IOException;

public interface Deserializable {

    int header();

    void prepare(ByteBuffer buffer);

    void deserialize(ByteBuffer buffer) throws IOException;
}
