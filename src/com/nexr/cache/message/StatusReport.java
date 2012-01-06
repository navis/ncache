package com.nexr.cache.message;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.nexr.cache.OutputCollector;

public class StatusReport extends Message {

    int[][] usages;

    public StatusReport() {}

    public StatusReport(byte namespace, int[][] usages) {
        this.nspace = namespace;
        this.usages = usages;
    }
    
    public byte opcode() {
        return Message.REPORT;
    }

    public void serialize(OutputCollector collector) throws IOException {
        ByteBuffer output = collector.temporary((usages.length << 3) + 4);
        output.putInt(usages.length);
        for (int[] usage : usages) {
            output.putInt(usage[0]);
            output.putInt(usage[1]);
        }
        output.flip();
    }

    public void deserialize(ByteBuffer buffer) throws IOException {
        usages = new int[buffer.getInt()][2];
        for (int i = 0; i < usages.length; i++) {
            usages[i][0] = buffer.getInt();
            usages[i][1] = buffer.getInt();
        }
    }
}
