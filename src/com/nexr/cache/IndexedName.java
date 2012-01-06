package com.nexr.cache;

import java.io.DataInput;
import java.io.IOException;
import java.io.DataOutput;

public class IndexedName {

    protected int index;
    protected String name;

    public IndexedName(DataInput input) throws IOException {
        read(input);
    }

    public IndexedName(int index, String name) {
        this.index = index;
        this.name = name;
    }

    public int index() {
        return index;
    }

    public String name() {
        return name;
    }

    public void read(DataInput input) throws IOException {
        index = input.readInt();
        name = input.readUTF();
    }

    public void write(DataOutput output) throws IOException {
        output.writeInt(index);
        output.writeUTF(name);
    }

    public String toString() {
        return index + "." + name;
    }
}
