package com.nexr.cache.message;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.nexr.cache.OutputCollector;
import com.nexr.cache.ServerEntity;
import com.nexr.cache.Utils;
import com.nexr.cache.message.request.Request;

public class Snapshot extends Message implements SourceAware, Aggregatable, Immediate, TransmitExpirable {

    private transient int expire;
    private transient ServerEntity source;

    private long[][][] generations;

    public Snapshot() {}

    public Snapshot(long[][][] generations) {
        this.generations = generations;
    }

    public long[][][] snapshot() {
        return generations;
    }

    public byte opcode() {
        return SNAPSHOT;
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

    public void serialize(OutputCollector collector) throws IOException {
        int nspace = generations.length;
        int partition = generations[0].length;
        int replication = generations[0][0].length;

        ByteBuffer buffer = collector.temporary((partition * replication * nspace) << 3 + 12);
        buffer.putInt(nspace);
        buffer.putInt(partition);
        buffer.putInt(replication);
        for (int i = 0; i < nspace; i++) {
            for (int j = 0; j < partition; j++) {
                for (int k = 0; k < replication; k++) {
                    buffer.putLong(generations[i][j][k]);
                }
            }
        }
        buffer.flip();
    }

    public void deserialize(ByteBuffer buffer) throws IOException {
        int nspace = buffer.getInt();
        int partition = buffer.getInt();
        int replication = buffer.getInt();
        generations = new long[nspace][partition][replication];
        for (int i = 0; i < nspace; i++) {
            for (int j = 0; j < partition; j++) {
                for (int k = 0; k < replication; k++) {
                    generations[i][j][k] = buffer.getLong();
                }
            }
        }
    }

    public String toString() {
        return source == null ? super.toString() : super.toString() + " <-- " + source;
    }

    public boolean isFinished(Request request) {
        return request.arrived();
    }

    public boolean isExpired() {
        return expire > 0 && expire <= Utils.currentSeconds();
    }
}
