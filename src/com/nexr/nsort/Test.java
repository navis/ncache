package com.nexr.nsort;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class Test {

    FileChannel rindex;
    FileChannel[] cindexes;

    public Test(int column) {
        cindexes = new FileChannel[column];
    }

    public void initialize(File directory) throws Exception {
        File rindexf = new File(directory, ".ix");
        rindex = FileChannel.open(rindexf.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        for (int i = 0; i < cindexes.length; i++) {
            File cindexf = new File(directory, i + ".ix");
            cindexes[i] = FileChannel.open(rindexf.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        }
    }

    public void write(int[][] index) {

    }
}
