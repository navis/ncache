package com.nexr.cache.message;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.nexr.cache.OutputCollector;

public interface Serializable {

    void serialize(OutputCollector collector) throws IOException;
}
