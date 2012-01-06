package com.nexr.cache;

import java.nio.ByteBuffer;

import static com.nexr.cache.Constants.MEMORY;
import com.nexr.cache.util.BufferUtil;
import com.nexr.cache.util.Generations;

public class RecordManager implements Record {

    int limitKB;
    MemoryPool memory;

    public RecordManager(MemoryPool memory) {
        this(memory, 256 << 10);
    }

    public RecordManager(MemoryPool memory, int limitKB) {
        this.memory = memory;
        this.limitKB = limitKB;
    }

    public long capacity() {
        return (long)limitKB << 10;     // KB to B
    }

    public String keyString(int[] mindex) {
        ByteBuffer region = memory.regionFor(mindex, KLENGTH_OFFSET, -1);
        return Utils.keyString(region);
    }

    public String valueString(int[] mindex) {
        ByteBuffer region = memory.regionFor(mindex, KLENGTH_OFFSET, -1);
        return Utils.valueString(region);
    }

    public String keyValueString(int[] mindex) {
        return keyValueString(mindex, (byte)':');
    }

    public String keyValueString(int[] mindex, byte delim) {
        ByteBuffer region = memory.regionFor(mindex, KLENGTH_OFFSET, -1);
        return Utils.kvString(region, delim);
    }

    public int expireTime(int[] mindex) {
        ByteBuffer buffer = memory.regionFor(mindex, false);
        return buffer.getInt(Region.toOffset(mindex) + EXPIRE_OFFSET);
    }

    public boolean isRemoved(int[] mindex) {
        ByteBuffer buffer = memory.regionFor(mindex, false);
        return (buffer.get(FLAGS_OFFSET) & FLAG_REMOVED) != 0;
    }

    public int[] information(int[] mindex) {
        ByteBuffer buffer = memory.regionFor(mindex, false);
        int hash = buffer.getInt(Region.toOffset(mindex) + HASH_OFFSET);
        long generation = buffer.getLong(Region.toOffset(mindex) + GSTAMP_OFFSET);

        return Generations.INDEX(generation, hash);
    }

    public int[] cropData(int[] mindex) {
        int offset = Region.toOffset(mindex);
        ByteBuffer buffer = memory.regionFor(mindex, false);
        int length = RECORD_HEADER_LEN + buffer.getShort(offset + KLENGTH_OFFSET);

        byte flags = buffer.get(offset + FLAGS_OFFSET);
        buffer.put(FLAGS_OFFSET, flags |= FLAG_REMOVED);

        int[] nindex = memory.allocate(length);
        ByteBuffer cropped = memory.regionFor(nindex, true);

        cropped.put(memory.regionFor(mindex, 0, length));
        memory.release(mindex);
        System.out.println("-- [RecordManager/cropData] " + Region.toString(mindex) + " --> " + Region.toString(nindex));
        return nindex;
    }
    
    public int[] cropData(int[][] eindex) {
        return eindex[MEMORY] = cropData(eindex[MEMORY]);
    }

    public boolean equals(int[] mindex1, int[] mindex2) {
        ByteBuffer buffer1 = memory.regionFor(mindex1, false);
        ByteBuffer buffer2 = memory.regionFor(mindex2, false);

        int offset1 = Region.toOffset(mindex1) + KLENGTH_OFFSET;
        int offset2 = Region.toOffset(mindex2) + KLENGTH_OFFSET;

        int length1 = buffer1.getShort(offset1);
        int length2 = buffer2.getShort(offset2);
        if (length1 != length2) {
            return false;
        }
        offset1 += KLENGTH_LENTH;
        offset2 += KLENGTH_LENTH;
        return BufferUtil.equals(buffer1, offset1, buffer2, offset2, length1);
    }

    public boolean equals(byte[] key, int[] stored) {
        ByteBuffer buffer = memory.regionFor(stored, false);

        int offset = Region.toOffset(stored) + KLENGTH_OFFSET;
        int length = buffer.getShort(offset);
        if (key.length != length) {
            return false;
        }
        offset += KLENGTH_LENTH;
        return BufferUtil.equals(key, buffer, offset);
    }

    public void release(int[] mindex) {
        memory.release(mindex);
    }
}
