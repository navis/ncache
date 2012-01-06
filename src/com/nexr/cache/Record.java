package com.nexr.cache;

public interface Record {

    int GSTAMP_OFFSET = 0;
    int GSTAMP_LENTH = 8;
    
    int GSTAMP_HIGH_OFFSET = 0;
    int GSTAMP_LOW_OFFSET = 4;

    int HASH_OFFSET = GSTAMP_OFFSET + GSTAMP_LENTH;
    int HASH_LENTH = 4;

    int EXPIRE_OFFSET = HASH_OFFSET + HASH_LENTH;
    int EXPIRE_LENTH = 4;

    int FLAGS_OFFSET = EXPIRE_OFFSET + EXPIRE_LENTH;
    int FLAGS_LENTH = 1;

    int FLAG_PERSIST = 0x01;
    int FLAG_REMOVED = 0x02;

    int KLENGTH_OFFSET = FLAGS_OFFSET + FLAGS_LENTH;
    int KLENGTH_LENTH = 2;

    int KEY_OFFSET = KLENGTH_OFFSET + KLENGTH_LENTH;

    int RECORD_HEADER_LEN = KEY_OFFSET;     // 19
    int CLIENT_HEADER_LEN = HASH_LENTH + EXPIRE_LENTH + FLAGS_LENTH + KLENGTH_LENTH;  // 11
}
