package com.nexr.cache;

import java.io.DataInput;
import java.io.IOException;

public class IndexedServerName extends IndexedName {

    public IndexedServerName(DataInput input) throws IOException {
        super(input);
    }

    public IndexedServerName(int index, String name) {
        super(index, name);
    }
}
