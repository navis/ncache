package com.nexr.cache.util;

import java.nio.ByteBuffer;

public class BufferUtil {

    public static boolean equals(ByteBuffer buffer1, int offset1, ByteBuffer buffer2, int offset2, int length) {
        for (int i = 0; i < length; i++) {
            if (buffer1.get(offset1 + i) != buffer2.get(offset2 + i)) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(byte[] buffer1, ByteBuffer buffer2, int offset2) {
        for (int i = 0; i < buffer1.length; i++) {
            if (buffer1[i] != buffer2.get(offset2 + i)) {
                return false;
            }
        }
        return true;
    }
}
