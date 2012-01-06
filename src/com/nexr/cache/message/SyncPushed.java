package com.nexr.cache.message;

public class SyncPushed extends StreamingMessage {

    public byte opcode() {
        return SYNC_DATA;
    }
}
