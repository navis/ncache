package com.nexr.cache.message;

import java.nio.ByteBuffer;

import com.nexr.cache.OutputCollector;

public abstract class Message extends AbstractDeserializable implements Serializable {

    public static final int HEADER_LENGTH = 12;
    public static final short MAGIC = 0x6f31;

    public static final int NEGO_CLIENT = -100;
    public static final int NEGO_PEER = -200;

    public static final int NEGO_SUCCESS = 0;
    public static final int NEGO_FAILURE = 1;

    public static final byte EOF = -1;

    public static final byte ACK = 0;
    public static final byte NACK = 1;

    public static final byte REQ_GET = 4;
    public static final byte REQ_STORE = 5;
    public static final byte REQ_REMOVE = 6;
    public static final byte REQ_FLUSH = 7;

    public static final byte RETRIEVED = 8;

    public static final byte FWD_STORE = 10;
    public static final byte FWD_REMOVE = 11;
    public static final byte FWD_SET_RECOVERED = 12;

    public static final byte REPORT = 20;
    public static final byte SNAPSHOT = 21;
    public static final byte SYNC_REQS = 22;
    public static final byte SYNC_DATA = 23;
    public static final byte SYNC_COMPLETED = 24;

    public static final byte CONFLICTED = 25;
    public static final byte SYNC_REQ = 26;

    public int callid;
    public byte nspace;

    private transient boolean canceled;

    public abstract byte opcode();

    public void cancelRequest() {
        canceled = true;
    }

    public boolean isCanceled() {
        return canceled;
    }

    public boolean isRequest() {
        return callid >= 0;     // include notify messages
    }

    public ByteBuffer[] asHeadered(ByteBuffer header, OutputCollector collector) {
        header.putShort(Message.MAGIC);
        header.put(opcode());
        header.put(nspace);
        header.putInt(callid);
        header.putInt(collector.length());
        header.flip();

        return asPackaged(header, collector);
    }

    protected ByteBuffer[] asPackaged(ByteBuffer header, OutputCollector collector) {
        return collector.collected(header);
    }

    public void transmitted() {
    }

    public String toString() {
        return MessageFactory.toString(opcode());
    }
}
