package com.nexr.cache.message;

import java.util.List;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.nexr.cache.ServerEntity;
import com.nexr.cache.OutputCollector;

public class SyncRequest extends Message implements SourceAware {

    int length;
    int[][][] requests;

    ServerEntity source;

    public SyncRequest() {}

    public SyncRequest(int length) {
        requests = new int[length][][];
    }

    public boolean isEmpty() {
        return length == 0;
    }

    public int[][][] requests() {
        return requests;
    }

    public void add(int namespace, List<int[]> pulling) {
        length += pulling.size();
        requests[namespace] = pulling.toArray(new int[pulling.size()][]);
    }

    public byte opcode() {
        return SYNC_REQS;
    }

    public void serialize(OutputCollector collector) throws IOException {
        ByteBuffer buffer = collector.temporary(4 + requests.length * 4 + length * 12);
        buffer.putInt(requests.length);
        for (int[][] request : requests) {
            buffer.putInt(request.length);
            for (int[] index : request) {
                buffer.putInt(index[0]);
                buffer.putInt(index[1]);
                buffer.putInt(index[2]);
            }
        }
        buffer.flip();
    }

    public void deserialize(ByteBuffer buffer) throws IOException {
        requests = new int[buffer.getInt()][][];
        for (int namespace = 0; namespace < requests.length; namespace++) {
            requests[namespace] = new int[buffer.getInt()][];
            for (int i = 0; i < requests[namespace].length; i++) {
                requests[namespace][i] = new int[]{buffer.getInt(), buffer.getInt(), buffer.getInt()};
                length++;
            }
        }
    }

    public int index() {
        return source == null ? -1 : source.index();
    }

    public ServerEntity source() {
        return source;
    }

    public void source(ServerEntity source) {
        this.source = source;
    }
}
