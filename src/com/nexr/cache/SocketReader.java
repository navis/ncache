package com.nexr.cache;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface SocketReader {
    ByteBuffer buffer();
    int read(ByteBuffer buffer) throws IOException;
    int readFully(ByteBuffer buffer) throws IOException;
    int[] allocate(int length);
    void release(int[] index);
}
